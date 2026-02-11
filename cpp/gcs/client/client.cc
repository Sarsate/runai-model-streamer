#include <algorithm>
#include <string>
#include <utility>
#include <optional>
#include <vector>
#include <future>
#include <memory>
#include <functional>
#include <iostream>
#include <cstring>
#include <thread>

#include "google/cloud/future.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/oauth2/credentials.h"
#include "google/cloud/common_options.h"
#include "google/cloud/status_or.h"

#include "google/cloud/storage/async/reader.h"
#include "google/cloud/storage/async/token.h"

#include "gcs/client/client.h"

#include "common/exception/exception.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"
#include "utils/fd/fd.h"

#include <shared_mutex>

#include <unistd.h> // For getpid()

namespace runai::llm::streamer::impl::gcs
{

GCSClient::GCSClient(const common::backend_api::ObjectClientConfig_t& config) :
    _stop(false),
    _responder(nullptr),
    _chunk_bytesize(config.default_storage_chunk_size)
{
    // std::cerr << "DEBUG: [PID=" << getpid() << "] GCSClient constructor called" << std::endl;
    if (_client_config.use_new_async_client) {
        // std::cerr << "DEBUG: [PID=" << getpid() << "] Initializing AsyncClient" << std::endl;
        _new_async_client = std::make_unique<google::cloud::storage_experimental::AsyncClient>(_client_config.options);
    } else {
        // std::cerr << "DEBUG: Initializing LEGACY AsyncGcsClient" << std::endl;
        _client = std::make_unique<AsyncGcsClient>(_client_config.options, _client_config.max_concurrency);
    }
    // std::cout << "DEBUG: Custom Run:ai Streamer build is active!" << std::endl;
    // std::cerr << "DEBUG: GCSClient constructor finished" << std::endl;
}

bool GCSClient::verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const
{
    // TODO: Verify credentials once they are passed in via config.
    return true;
}

common::backend_api::Response GCSClient::async_read_response()
{
    if (_responder == nullptr)
    {
        std::cerr << "DEBUG WARNING: Requesting response with uninitialized responder" << std::endl;
        return common::ResponseCode::FinishedError;
    }

    return _responder->pop();
}

common::ResponseCode write_stream_to_buffer(
        google::cloud::storage::ObjectReadStream && stream,
        char * dest_buffer,
        size_t bytesize,
        common::backend_api::ObjectRequestId_t request_id) {
    size_t bytes_received = 0;
    char* ptr = dest_buffer;
    size_t remaining = bytesize;

    // Robust read loop
    while (remaining > 0 && stream) {
        stream.read(ptr, remaining);
        std::streamsize count = stream.gcount();
        bytes_received += count;
        ptr += count;
        remaining -= count;
    }
    
    stream.Close();

    if (bytes_received != bytesize) {
        // std::cerr << "DEBUG ERROR: GCS ReadObject request " << request_id 
        //           << " failed. Received " << bytes_received << " bytes, expected " << bytesize << "." << std::endl;
        return common::ResponseCode::FileAccessError;
    }
    if (stream.bad()) {
        // const auto & err = stream.status();
        // std::cerr << "DEBUG ERROR: GCS stream bad for request " << request_id 
        //           << ". Code: " << err.code() << ", Message: " << err.message() << std::endl;
        return common::ResponseCode::FileAccessError;
    }

    return common::ResponseCode::Success;
}

void GCSClient::PreOpen(const std::vector<std::string>& paths) {
    if (!_client_config.use_new_async_client) {
        return;
    }

    std::vector<google::cloud::future<void>> futures;

    for (const auto& path : paths) {
        const auto uri = common::s3::StorageUri(path);
        std::string bucket_name(uri.bucket);
        std::string path_name(uri.path);
        std::string key = path;

        // Check Cache (Shared Lock)
        {
            std::shared_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
            if (_descriptors.find(key) != _descriptors.end()) {
                continue;
            }
        }

            // Initiate Open
            // std::cerr << "DEBUG: PreOpen initiating Open for " << key << std::endl;
            auto f = _new_async_client->Open(google::cloud::storage_experimental::BucketName(bucket_name), path_name)
                .then([this, key](auto f) {
                    auto result = f.get();
                    std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
                    
                    if (!result) {
                        // std::cerr << "DEBUG ERROR: PreOpen failed for " << key << ": " << result.status().message() << std::endl;
                    } else {
                        descriptor = std::make_shared<google::cloud::storage_experimental::ObjectDescriptor>(*std::move(result));
                    }
        
                    {
                        std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                        if (descriptor) {
                            _descriptors[key] = descriptor;
                        }
                        // Also handle any pending opens that might have been queued by async_read
                        // in a race condition (though Workload structure avoids this)
                        auto it = _pending_opens.find(key);
                        if (it != _pending_opens.end()) {
                            auto callbacks = std::move(it->second);
                            _pending_opens.erase(it);
                            lock.unlock(); // Unlock before callbacks
                            for (const auto& cb : callbacks) {
                                cb(descriptor);
                            }
                        }
                    }
                });
            
            futures.push_back(std::move(f));
        }
        
        if (!futures.empty()) {
            // std::cerr << "DEBUG: Waiting for " << futures.size() << " PreOpen operations to complete..." << std::endl;
            for (auto& f : futures) {
                f.get();
            }
            // std::cerr << "DEBUG: All PreOpen operations completed." << std::endl;
        }}

GCSClient::~GCSClient() {
    // std::cerr << "DEBUG: GCSClient destructor called" << std::endl;
}

namespace {
    void StreamToBuffer(google::cloud::storage_experimental::AsyncReader reader,
                        google::cloud::storage_experimental::AsyncToken token,
                        char* buffer,
                        size_t remaining_in_chunk,
                        common::backend_api::ObjectRequestId_t request_id,
                        std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder,
                        std::shared_ptr<std::atomic<unsigned>> pending_chunks,
                        std::shared_ptr<std::atomic<bool>> is_success) {
        
        reader.Read(std::move(token)).then(
            [reader = std::move(reader), buffer, remaining_in_chunk, request_id, responder, pending_chunks, is_success]
            (auto f) mutable {
                auto result = f.get();
                if (!result) {
                    // std::cerr << "DEBUG ERROR: StreamToBuffer failed for request " << request_id << ": " << result.status().message() << std::endl;
                    if (is_success->exchange(false)) {
                        responder->push({request_id, common::ResponseCode::FileAccessError});
                    }
                    return;
                }

                auto& payload = result->first;
                auto& next_token = result->second;

                size_t bytes_copied = 0;
                for (const auto& chunk : payload.contents()) {
                    if (bytes_copied + chunk.size() > remaining_in_chunk) {
                        // std::cerr << "DEBUG ERROR: StreamToBuffer overflow for request " << request_id << std::endl;
                        if (is_success->exchange(false)) {
                            responder->push({request_id, common::ResponseCode::FileAccessError});
                        }
                        return;
                    }
                    std::memcpy(buffer + bytes_copied, chunk.data(), chunk.size());
                    bytes_copied += chunk.size();
                }

                if (next_token.valid()) {
                    StreamToBuffer(std::move(reader), std::move(next_token), 
                                   buffer + bytes_copied, remaining_in_chunk - bytes_copied, 
                                   request_id, responder, pending_chunks, is_success);
                } else {
                    if (bytes_copied != remaining_in_chunk) {
                        // std::cerr << "DEBUG ERROR: StreamToBuffer incomplete read for request " << request_id 
                        //           << ". Expected " << remaining_in_chunk << ", got " << bytes_copied << std::endl;
                         if (is_success->exchange(false)) {
                            responder->push({request_id, common::ResponseCode::FileAccessError});
                        }
                        return;
                    }

                    if (pending_chunks->fetch_sub(1) == 1) {
                        responder->push({request_id, common::ResponseCode::Success});
                    }
                }
            });
    }
}

common::ResponseCode GCSClient::async_read(const char* path, common::backend_api::ObjectRange_t range, char* destination_buffer, common::backend_api::ObjectRequestId_t request_id)
{
    // std::cerr << "DEBUG: async_read called for request_id " << request_id << " path=" << path 
    //           << " range=[" << range.offset << ", " << range.length << "]" << std::endl;
    if (_responder == nullptr)
    {
        _responder = std::make_shared<Responder>(1);
    }
    else
    {
        _responder->increment(1);
    }

    if (_client_config.use_new_async_client) {
        // std::cerr << "DEBUG: async_read using NEW AsyncClient" << std::endl;
        
        // We use a single chunk for the whole range in the new async client
        auto pending_chunks = std::make_shared<std::atomic<unsigned>>(1);
        auto is_success = std::make_shared<std::atomic<bool>>(true);

        const auto uri = common::s3::StorageUri(path);
        std::string bucket_name(uri.bucket);
        std::string path_name(uri.path);
        std::string key = path; // Use path as key as requested

        // Helper to trigger read on a descriptor
        auto trigger_read = [=](std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor) {
             auto read_result = descriptor->Read(range.offset, range.length);
             
             StreamToBuffer(std::move(read_result.first), std::move(read_result.second), 
                            destination_buffer, range.length, request_id, _responder, pending_chunks, is_success);
        };

        // Optimization: Lock-free check.
        // Since we PreOpen and Wait for all files in the workload before starting async_read,
        // the _descriptors map should be populated and immutable (for these keys) during this phase.
        // This avoids lock contention in the high-frequency read loop.
        {
            auto it = _descriptors.find(key);
            if (it != _descriptors.end()) {
                trigger_read(it->second);
                return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
            }
        }

        // Fallback to locking mechanism if not found (e.g. if PreOpen failed or wasn't called)
        {
            // 1. Check Cache (Shared Lock)
            std::shared_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
            auto it = _descriptors.find(key);
            if (it != _descriptors.end()) {
                auto descriptor = it->second;
                lock.unlock();
                // std::cerr << "DEBUG: Cache hit for descriptor: " << key << std::endl;
                trigger_read(descriptor);
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
            // std::cerr << "DEBUG: Cache hit (after lock upgrade) for descriptor: " << key << std::endl;
            trigger_read(descriptor);
            return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
        }

        // 2. Check/Add to Pending Opens: Coalesce requests for the same object.
        // If an Open is already in progress, queue this request's callback.
        auto callback = [=](std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor) {
            if (!descriptor) {
                // Open failed, fail this specific request
                if (is_success->exchange(false)) {
                    _responder->push({request_id, common::ResponseCode::FileAccessError});
                }
                return;
            }
            trigger_read(descriptor);
        };

        auto& pending = _pending_opens[key];
        bool is_first_request = pending.empty();
        pending.push_back(std::move(callback));

        if (!is_first_request) {
            // An Open is already in progress. This request is now queued.
            // std::cerr << "DEBUG: Coalescing open request for: " << key << " (queue size: " << pending.size() << ")" << std::endl;
            return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
        }

        lock.unlock();

        // 3. Initiate Open: We are the first request, so we trigger the Open.
        // std::cerr << "DEBUG: Cache miss. Initiating Open for " << key << std::endl;
        _new_async_client->Open(google::cloud::storage_experimental::BucketName(bucket_name), path_name)
            .then([this, key, request_id](auto f) {
                auto result = f.get();
                std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
                
                if (!result) {
                    // std::cerr << "DEBUG ERROR: Failed to open object descriptor for request " << request_id << ": " << result.status().message() << std::endl;
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
                
                // std::cerr << "DEBUG: Open finished for " << key << ". Dispatching to " << callbacks.size() << " waiting requests." << std::endl;
                for (const auto& cb : callbacks) {
                    cb(descriptor);
                }
            });

        return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
    }

    char * buffer_ = destination_buffer;
    // split range into chunks
    size_t size = std::max(1UL, range.length/_chunk_bytesize);
    // std::cerr << "DEBUG SPAM: Number of chunks is: " << size << std::endl;

    // each range is divided into chunks (size is the number of chunks)
    // when all the chunks have been read successfuly the response for that range is pushed to the responder

    auto counter = std::make_shared< std::atomic<unsigned> >(size);
    // success flag for the current range is passed to the client
    auto is_success = std::make_shared< std::atomic<bool> >(true);

    const auto uri = common::s3::StorageUri(path);

    std::string bucket_name(uri.bucket);
    std::string path_name(uri.path);

    size_t total_ = range.length;
    size_t offset_ = range.offset;

    for (unsigned i = 0; i < size && !_stop; ++i)
    {
        size_t bytesize_ = (i == size - 1 ? total_ : _chunk_bytesize);

        _client->ReadObjectAsync(bucket_name, path_name, google::cloud::storage::ReadRange(offset_, offset_ + bytesize_)).then(
            [dest_buffer = buffer_, responder = _responder, request_id, bytesize_, counter, is_success](auto f) {
            auto stream = f.get();
            auto response_code = write_stream_to_buffer(std::move(stream), dest_buffer, bytesize_, request_id);
            if (response_code == common::ResponseCode::Success)
            {
                const auto running = counter->fetch_sub(1);
                // std::cerr << "DEBUG SPAM: Async read request " << request_id << " succeeded - " << running << " running" << std::endl;
                // send success response only if all the requests have succeeded
                // note that unsuccessful attempts do not update the counter
                if (running == 1)
                {
                    common::backend_api::Response r(request_id, response_code);
                    responder->push(std::move(r));
                }
            }
            else
            {
                // Note: currently a failure to read any sub range fails the entire read request
                //       a retry mechanism should be added for failed reads
                bool previous = is_success->exchange(false);
                // send error response only once
                if (previous)
                {
                    common::backend_api::Response r(request_id, response_code);
                    responder->push(std::move(r));
                }
            }
        });

        total_ -= bytesize_;
        offset_ += bytesize_;
        buffer_ += bytesize_;
    }

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
}

void GCSClient::stop()
{
    _stop = true;
    if (_responder != nullptr)
    {
        _responder->stop();
    }
}

}; // namespace runai::llm::streamer::impl::gcs
