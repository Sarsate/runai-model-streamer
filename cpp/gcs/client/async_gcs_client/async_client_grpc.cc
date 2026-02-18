#include "gcs/client/async_gcs_client/async_client_grpc.h"

#include <iostream>
#include <cstring>
#include <thread>
#include <chrono>
#include <algorithm>

#include "google/cloud/future.h"
#include "google/cloud/storage/client.h"

#include "common/storage_uri/storage_uri.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl::gcs
{

AsyncClientGrpc::AsyncClientGrpc(const ClientConfiguration& config) :
    _thread_pool([this](ReadTask&& task, std::atomic<bool>& stopped) {
        ExecuteTask(std::move(task), stopped);
    }, config.max_concurrency)
{
    LOG(DEBUG) << "Initializing AsyncClientGrpc with ThreadPool size: " << config.max_concurrency;
    _client = std::make_shared<google::cloud::storage_experimental::AsyncClient>(config.options);
    _monitor_thread = std::thread(&AsyncClientGrpc::Monitor, this);

    // Warmup: Trigger Auth and Connection Establishment
    auto warmup_f = _client->Open(google::cloud::storage_experimental::BucketName("runai-warmup-dummy-bucket"), "dummy-object");
}

AsyncClientGrpc::~AsyncClientGrpc()
{
    Stop();
}

void AsyncClientGrpc::Stop()
{
    _stop = true;
    _monitor_cv.notify_all();

    if (_monitor_thread.joinable()) {
        _monitor_thread.join();
    }
}

std::shared_future<std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> 
AsyncClientGrpc::TriggerOpen(const std::string& key, const std::string& bucket, const std::string& path) {
    std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);

    // 1. Check Cache
    auto it = _descriptors.find(key);
    if (it != _descriptors.end()) {
        it->second.last_used = std::chrono::steady_clock::now();
        return it->second.descriptor_future;
    }

    // 2. Initiate New Open
    auto p = std::make_shared<std::promise<std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>>>();
    std::shared_future<std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> fut = p->get_future();
    
    // Insert into cache immediately with the future
    _descriptors[key] = {fut, std::chrono::steady_clock::now()};

    // We can hold the lock while firing Open because Open returns a future instantly.
    // It does NOT block.
    _client->Open(google::cloud::storage_experimental::BucketName(bucket), path)
        .then([this, key, p](auto f) {
            auto result = f.get();
            std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
            
            if (!result) {
                LOG(ERROR) << "Failed to open descriptor for " << key << ": " << result.status().message();
                
                // On failure, remove the entry so it can be retried later
                try {
                    std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                    _descriptors.erase(key);
                } catch (...) {
                    LOG(WARNING) << "Exception in TriggerOpen failure cleanup (likely shutdown race)";
                }
            } else {
                descriptor = std::make_shared<google::cloud::storage_experimental::ObjectDescriptor>(*std::move(result));
            }

            // Fulfill the promise, unblocking all waiting threads
            p->set_value(descriptor);
        });

    return fut;
}

std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> 
AsyncClientGrpc::GetDescriptor(const std::string& key, const std::string& bucket, const std::string& path) {
    // Uses the coalescing logic
    return TriggerOpen(key, bucket, path).get();
}

void AsyncClientGrpc::ExecuteTask(ReadTask&& task, std::atomic<bool>& stopped) {
    if (stopped) return;
    _active_tasks++;

    // Helper to cleanup on exit
    // We use a lambda to ensure we ALWAYS decrement, no matter where we return.
    auto finalize = [&](bool success) {
        _active_tasks--;
        
        // Decrement pending chunks
        // If we are the LAST chunk (fetch_sub returns 1) AND the request is overall successful...
        if (task.pending_chunks->fetch_sub(1) == 1) {
            if (task.is_success->load()) {
                LOG(DEBUG) << "Finished reading request " << task.request_id << " " << task.key << " offset " << task.offset << " size " << task.length;
                task.responder->push({task.request_id, common::ResponseCode::Success});
            }
        }
    };

    auto descriptor = GetDescriptor(task.key, task.bucket_name, task.path_name);
    if (!descriptor) {
        if (task.is_success->exchange(false)) {
            task.responder->push({task.request_id, common::ResponseCode::FileAccessError});
        }
        finalize(false);
        return;
    }

    try {
        // 1. Get Reader/Token (Instant)
        auto reader_token_pair = descriptor->Read(task.offset, task.length);
        auto reader = std::move(reader_token_pair.first);
        auto token = std::move(reader_token_pair.second);

        size_t bytes_copied = 0;

        // Pipelining Setup: Start the first read immediately
        if (!token.valid()) {
            if (task.length == 0) {
                finalize(true);
                return;
            }
            // Should not happen for non-zero length unless file is truncated/error
            LOG(ERROR) << "Invalid token for non-zero request " << task.request_id;
            finalize(false);
            return;
        }
        auto pending_read = reader.Read(std::move(token));

        // 2. Read Loop
        
        while (!stopped) {
            // Block until the *current* chunk arrives, with timeout logging
            std::future_status status;
            do {
                status = pending_read.wait_for(std::chrono::milliseconds(100));
            } while (status == std::future_status::timeout && !stopped);

            if (stopped) return;

            auto result = pending_read.get(); 

            if (!result) {
                 LOG(ERROR) << "Read failed for request " << task.request_id << ": " << result.status().message();
                 if (task.is_success->exchange(false)) {
                    task.responder->push({task.request_id, common::ResponseCode::FileAccessError});
                }
                finalize(false);
                return;
            }

            auto& payload = result->first;
            auto next_token = std::move(result->second);

            // PIPELINING OPTIMIZATION: 
            // Kick off the *next* read request immediately, BEFORE we spend time copying data.
            // This allows the network download of Chunk N+1 to happen in parallel with the CPU copy of Chunk N.
            bool has_more = next_token.valid();
            if (has_more && !stopped) {
                pending_read = reader.Read(std::move(next_token));
            }

            for (const auto& chunk : payload.contents()) {
                if (bytes_copied + chunk.size() > task.length) {
                    // Overflow
                     if (task.is_success->exchange(false)) {
                        task.responder->push({task.request_id, common::ResponseCode::FileAccessError});
                    }
                    finalize(false);
                    return;
                }
                
                // Use non-temporal memcpy optimization
                std::memcpy(task.buffer + bytes_copied, chunk.data(), chunk.size());
                
                bytes_copied += chunk.size();
            }

            if (!has_more) break;
        }

        if (bytes_copied != task.length) {
             LOG(ERROR) << "Incomplete read for request " << task.request_id;
             if (task.is_success->exchange(false)) {
                task.responder->push({task.request_id, common::ResponseCode::FileAccessError});
            }
            finalize(false);
            return;
        }
    } catch (...) {
         if (task.is_success->exchange(false)) {
            task.responder->push({task.request_id, common::ResponseCode::FileAccessError});
        }
        finalize(false);
        return;
    }

    // Success path
    finalize(true);
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

    // Shared state
    auto pending_chunks = std::make_shared<std::atomic<unsigned>>(num_chunks);
    auto is_success = std::make_shared<std::atomic<bool>>(true);

    const auto uri = common::s3::StorageUri(path);
    std::string bucket_name(uri.bucket);
    std::string path_name(uri.path);
    std::string key = path; 

    size_t current_offset = range.offset;
    char* current_buffer = destination_buffer;
    size_t remaining = range.length;

    for (size_t i = 0; i < num_chunks; ++i) {
        size_t chunk_len = std::min(CHUNK_SIZE, remaining);
        
        ReadTask task{
            bucket_name,
            path_name,
            key,
            current_offset,
            chunk_len,
            current_buffer,
            request_id,
            responder,
            pending_chunks,
            is_success
        };

        _thread_pool.push(std::move(task));
        
        current_offset += chunk_len;
        current_buffer += chunk_len;
        remaining -= chunk_len;
    }

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
}

void AsyncClientGrpc::Monitor() {
    while (!_stop) {
        size_t descriptors_count = 0;
        {
            std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
            auto now = std::chrono::steady_clock::now();
            for (auto it = _descriptors.begin(); it != _descriptors.end(); ) {
                auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.last_used).count();
                
                // Only expire if idle > 1s AND no other thread is holding it (use_count == 1 means only the map/future holds it)
                bool expired = false;
                if (idle_time >= 1) {
                    auto& fut = it->second.descriptor_future;
                    // Check if future is ready (using 0 timeout wait)
                    if (fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                        const auto& descriptor = fut.get();
                        // If descriptor is null (failed open that wasn't cleaned up) or use_count is 1
                        if (!descriptor || descriptor.use_count() == 1) {
                            expired = true;
                        }
                    }
                }

                if (expired) {
                    it = _descriptors.erase(it);
                } else {
                    ++it;
                }
            }
            descriptors_count = _descriptors.size();
        }
        
        LOG(INFO) << "AsyncClientGrpc Monitor: "
                    << "Active Tasks: " << _active_tasks.load() << ", "
                    << "Cached Descriptors: " << descriptors_count;
        
        std::unique_lock<std::mutex> lock(_monitor_mutex);
        _monitor_cv.wait_for(lock, std::chrono::seconds(2), [this] { return _stop.load(); });
    }
}

} // namespace runai::llm::streamer::impl::gcs
