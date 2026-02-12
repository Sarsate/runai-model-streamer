#include "gcs/client/async_gcs_client/async_client_grpc.h"

#include <iostream>
#include <cstring>
#include <thread>

#include "google/cloud/future.h"
#include "google/cloud/storage/client.h"

#include "common/storage_uri/storage_uri.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl::gcs
{

AsyncClientGrpc::AsyncClientGrpc(const ClientConfiguration& config)
{
    LOG(DEBUG) << "Initializing AsyncClientGrpc instance with dedicated AsyncClient";
    _client = std::make_shared<google::cloud::storage_experimental::AsyncClient>(config.options);
    _monitor_thread = std::thread(&AsyncClientGrpc::Monitor, this);
    _worker_thread = std::thread(&AsyncClientGrpc::WorkerLoop, this);
}

AsyncClientGrpc::~AsyncClientGrpc()
{
    Stop();
}

void AsyncClientGrpc::Stop()
{
    _stop = true;
    _queue_cv.notify_all(); // Wake up worker to exit
    _semaphore_cv.notify_all(); // Wake up any waiting permits

    if (_monitor_thread.joinable()) {
        _monitor_thread.join();
    }
    if (_worker_thread.joinable()) {
        _worker_thread.join();
    }
}

void AsyncClientGrpc::PreOpen(const std::vector<std::string>& paths)
{
    for (const auto& path : paths) {
        const auto uri = common::s3::StorageUri(path);
        std::string bucket_name(uri.bucket);
        std::string path_name(uri.path);
        std::string key = path;

        // 1. Check Cache (Shared Lock)
        {
            std::shared_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
            if (_descriptors.find(key) != _descriptors.end()) {
                continue;
            }
        }

        // 2. Check/Add to Pending Opens (Exclusive Lock)
        std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
        
        // Double check cache
        if (_descriptors.find(key) != _descriptors.end()) {
            continue;
        }

        auto& pending = _pending_opens[key];
        bool is_first_request = pending.empty();
        
        // Add a dummy callback so subsequent Reads see this as "in progress"
        pending.push_back([](std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>){});

        if (!is_first_request) {
            // Open already initiated by Read or another PreOpen
            continue;
        }

        lock.unlock();

        // 3. Initiate Open (Async)
        _active_opens++;
        _client->Open(google::cloud::storage_experimental::BucketName(bucket_name), path_name)
            .then([this, key](auto f) {
                _active_opens--;
                auto result = f.get();
                std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
                
                if (!result) {
                    LOG(ERROR) << "PreOpen failed for " << key << ": " << result.status().message();
                } else {
                    descriptor = std::make_shared<google::cloud::storage_experimental::ObjectDescriptor>(*std::move(result));
                }
    
                // 4. Complete and Notify
                std::vector<std::function<void(std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>)>> callbacks;
                {
                    std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                    if (descriptor) {
                        _descriptors[key] = descriptor;
                    }
                    
                    auto it = _pending_opens.find(key);
                    if (it != _pending_opens.end()) {
                        callbacks = std::move(it->second);
                        _pending_opens.erase(it);
                    }
                }
                
                for (const auto& cb : callbacks) {
                    cb(descriptor);
                }
            });
    }
}

void AsyncClientGrpc::StreamToBuffer(
    google::cloud::storage_experimental::AsyncReader reader,
    google::cloud::storage_experimental::AsyncToken token,
    char* buffer,
    size_t remaining_in_chunk,
    common::backend_api::ObjectRequestId_t request_id,
    std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder,
    std::shared_ptr<std::atomic<unsigned>> pending_chunks,
    std::shared_ptr<std::atomic<bool>> is_success)
{
    // Start the first read and attach the completion handler
    _active_streams++;
    auto f = reader.Read(std::move(token));
    OnReadComplete(std::move(f), std::move(reader), buffer, remaining_in_chunk, request_id, responder, pending_chunks, is_success);
}

void AsyncClientGrpc::OnReadComplete(
    google::cloud::future<google::cloud::StatusOr<std::pair<google::cloud::storage_experimental::ReadPayload, google::cloud::storage_experimental::AsyncToken>>> f,
    google::cloud::storage_experimental::AsyncReader reader,
    char* buffer,
    size_t remaining_in_chunk,
    common::backend_api::ObjectRequestId_t request_id,
    std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder,
    std::shared_ptr<std::atomic<unsigned>> pending_chunks,
    std::shared_ptr<std::atomic<bool>> is_success)
{
    f.then([this, reader = std::move(reader), buffer, remaining_in_chunk, request_id, responder, pending_chunks, is_success](auto f) mutable {
        _active_streams--;
        if (_stop) return;

        auto result = f.get();
        if (!result) {
            _active_reads--;
            ReleaseReadPermit();
            LOG(ERROR) << "StreamToBuffer failed for request " << request_id << ": " << result.status().message() << ". Active reads: " << _active_reads;
            if (is_success->exchange(false)) {
                responder->push({request_id, common::ResponseCode::FileAccessError});
            }
            return;
        }

        auto& payload = result->first;
        auto& next_token = result->second;

        // --- PIPELINING START ---
        // Initiate the next read BEFORE processing the current chunk.
        // This ensures the network download overlaps with the memory copy below.
        google::cloud::future<google::cloud::StatusOr<std::pair<google::cloud::storage_experimental::ReadPayload, google::cloud::storage_experimental::AsyncToken>>> next_read_future;
        bool has_next = next_token.valid();
        
        if (has_next) {
            _active_streams++;
            next_read_future = reader.Read(std::move(next_token));
        }
        // --- PIPELINING END ---

        size_t bytes_copied = 0;
        for (const auto& chunk : payload.contents()) {
            if (bytes_copied + chunk.size() > remaining_in_chunk) {
                _active_reads--;
                ReleaseReadPermit();
                LOG(ERROR) << "StreamToBuffer overflow for request " << request_id;
                if (is_success->exchange(false)) {
                    responder->push({request_id, common::ResponseCode::FileAccessError});
                }
                return;
            }
            std::memcpy(buffer + bytes_copied, chunk.data(), chunk.size());
            bytes_copied += chunk.size();
        }

        if (has_next) {
            // Recurse using the already-running future
            OnReadComplete(std::move(next_read_future), std::move(reader), 
                           buffer + bytes_copied, remaining_in_chunk - bytes_copied, 
                           request_id, responder, pending_chunks, is_success);
        } else {
            // Completion Logic
            _active_reads--;
            ReleaseReadPermit();
            if (bytes_copied != remaining_in_chunk) {
                LOG(ERROR) << "StreamToBuffer incomplete read for request " << request_id 
                           << ". Expected " << remaining_in_chunk << ", got " << bytes_copied
                           << ". Active reads: " << _active_reads;
                 if (is_success->exchange(false)) {
                    responder->push({request_id, common::ResponseCode::FileAccessError});
                }
                return;
            }

            if (pending_chunks->fetch_sub(1) == 1) {
                // LOG(DEBUG) << "Stream finished for request " << request_id << " (" << remaining_in_chunk << " bytes)";
                responder->push({request_id, common::ResponseCode::Success});
            }
        }
    });
}

common::ResponseCode AsyncClientGrpc::Read(
    const std::string& path,
    common::backend_api::ObjectRange_t range,
    char* destination_buffer,
    common::backend_api::ObjectRequestId_t request_id,
    std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder)
{
    const size_t CHUNK_SIZE = 200 * 1024 * 1024; // 200 MiB
    size_t total_length = range.length;
    size_t num_chunks = (total_length + CHUNK_SIZE - 1) / CHUNK_SIZE;

    // Shared state for the entire request (spanning multiple chunks)
    auto pending_chunks = std::make_shared<std::atomic<unsigned>>(num_chunks);
    auto is_success = std::make_shared<std::atomic<bool>>(true);

    const auto uri = common::s3::StorageUri(path);
    std::string bucket_name(uri.bucket);
    std::string path_name(uri.path);
    std::string key = path; 

    // Helper to trigger read on a descriptor for a specific chunk
    auto trigger_read_chunk = [this, request_id, responder, pending_chunks, is_success](
        std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor,
        size_t offset, size_t length, char* buffer) {
            
            // Push to queue (Non-blocking for application)
            {
                std::unique_lock<std::mutex> lock(_queue_mutex);
                _request_queue.push({descriptor, offset, length, buffer, request_id, responder, pending_chunks, is_success});
            }
            _queue_cv.notify_one();
    };

    // Callback that launches ALL chunks once the descriptor is ready
    auto launch_all_chunks = [trigger_read_chunk, range, destination_buffer, CHUNK_SIZE, num_chunks](
        std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor) {
            if (!descriptor) return; // Error handled by caller/wrapper

            size_t current_offset = range.offset;
            char* current_buffer = destination_buffer;
            size_t remaining = range.length;

            for (size_t i = 0; i < num_chunks; ++i) {
                size_t chunk_len = std::min(CHUNK_SIZE, remaining);
                trigger_read_chunk(descriptor, current_offset, chunk_len, current_buffer);
                
                current_offset += chunk_len;
                current_buffer += chunk_len;
                remaining -= chunk_len;
            }
    };

    // 1. Check Cache (Shared Lock)
    {
        std::shared_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
        auto it = _descriptors.find(key);
        if (it != _descriptors.end()) {
            auto descriptor = it->second;
            lock.unlock();
            launch_all_chunks(descriptor);
            return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
        }
    }

    // Cache Miss - Upgrade to Exclusive Lock
    std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);

    // 1b. Double Check Cache (Exclusive Lock)
    auto it = _descriptors.find(key);
    if (it != _descriptors.end()) {
        auto descriptor = it->second;
        lock.unlock();
        launch_all_chunks(descriptor);
        return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
    }

    // 2. Check/Add to Pending Opens
    auto callback = [launch_all_chunks, responder, request_id, is_success](std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor) {
        if (!descriptor) {
            // Open failed, fail this specific request
            if (is_success->exchange(false)) {
                responder->push({request_id, common::ResponseCode::FileAccessError});
            }
            return;
        }
        launch_all_chunks(descriptor);
    };

    auto& pending = _pending_opens[key];
    bool is_first_request = pending.empty();
    pending.push_back(std::move(callback));

    if (!is_first_request) {
        // An Open is already in progress. This request is now queued.
        return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
    }

    lock.unlock();

    // 3. Initiate Open
    _active_opens++;
    _client->Open(google::cloud::storage_experimental::BucketName(bucket_name), path_name)
        .then([this, key, request_id](auto f) {
            _active_opens--;
            auto result = f.get();
            std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
            
            if (!result) {
                LOG(ERROR) << "Failed to open object descriptor for request " << request_id << ": " << result.status().message();
                // descriptor remains null
            } else {
                descriptor = std::make_shared<google::cloud::storage_experimental::ObjectDescriptor>(*std::move(result));
            }

            // 4. Fan-Out: Execute all queued callbacks.
            std::vector<std::function<void(std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>)>> callbacks;
            {
                std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                
                if (descriptor) {
                    _descriptors[key] = descriptor; // Cache the successful descriptor
                }
                
                // Retrieve and remove the pending callbacks for this key
                auto it = _pending_opens.find(key);
                if (it != _pending_opens.end()) {
                    callbacks = std::move(it->second);
                    _pending_opens.erase(it);
                }
            }
            
            for (const auto& cb : callbacks) {
                cb(descriptor);
            }
        });

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
}


void AsyncClientGrpc::WorkerLoop() {
    while (!_stop) {
        std::unique_lock<std::mutex> lock(_queue_mutex);
        _queue_cv.wait(lock, [this] {
            return !_request_queue.empty() || _stop;
        });

        if (_stop) return;

        // Process up to N requests at once to avoid constant locking/unlocking?
        // No, keep it simple: pop one, process one.
        
        ReadRequest req = _request_queue.front();
        _request_queue.pop();
        lock.unlock(); // Release queue lock before acquiring permit

        // This will block if we hit concurrency limit
        AcquireReadPermit();

        if (_stop) {
            if (req.is_success->exchange(false)) {
                req.responder->push({req.request_id, common::ResponseCode::FinishedError});
            }
            return;
        }

        _active_reads++;
        auto read_result = req.descriptor->Read(req.offset, req.length);
        StreamToBuffer(std::move(read_result.first), std::move(read_result.second), 
                       req.buffer, req.length, req.request_id, req.responder, req.pending_chunks, req.is_success);
    }
}

void AsyncClientGrpc::AcquireReadPermit() {
    std::unique_lock<std::mutex> lock(_semaphore_mutex);
    _semaphore_cv.wait(lock, [this] {
        return _active_reads < _max_concurrent_reads || _stop;
    });
}

void AsyncClientGrpc::ReleaseReadPermit() {
    _semaphore_cv.notify_one();
}

void AsyncClientGrpc::Monitor() {
    while (!_stop) {
        size_t descriptors_count = 0;
        size_t pending_opens_count = 0;
        {
            std::shared_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
            descriptors_count = _descriptors.size();
            pending_opens_count = _pending_opens.size();
        }
        LOG(INFO) << "AsyncClientGrpc Monitor: "
                    << "Active Opens: " << _active_opens.load() << ", "
                    << "Active Reads: " << _active_reads.load() << ", "
                    << "Active Streams: " << _active_streams.load() << ", "
                    << "Cached Descriptors: " << descriptors_count << ", "
                    << "Pending Coalesced Opens: " << pending_opens_count;
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}

} // namespace runai::llm::streamer::impl::gcs
