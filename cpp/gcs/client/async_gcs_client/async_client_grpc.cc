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
    LOG(DEBUG) << "Initializing AsyncClientGrpc";
    _client = std::make_unique<google::cloud::storage_experimental::AsyncClient>(config.options);
}

AsyncClientGrpc::~AsyncClientGrpc() = default;

void AsyncClientGrpc::Stop()
{
    _stop = true;
}

void AsyncClientGrpc::PreOpen(const std::vector<std::string>& paths)
{
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
        auto f = _client->Open(google::cloud::storage_experimental::BucketName(bucket_name), path_name)
            .then([this, key](auto f) {
                auto result = f.get();
                std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
                
                if (!result) {
                    LOG(ERROR) << "PreOpen failed for " << key << ": " << result.status().message();
                } else {
                    descriptor = std::make_shared<google::cloud::storage_experimental::ObjectDescriptor>(*std::move(result));
                }
    
                {
                    std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                    if (descriptor) {
                        _descriptors[key] = descriptor;
                    }
                    // Also handle any pending opens that might have been queued by Read
                    // in a race condition
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
        for (auto& f : futures) {
            f.get();
        }
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
    reader.Read(std::move(token)).then(
        [this, reader = std::move(reader), buffer, remaining_in_chunk, request_id, responder, pending_chunks, is_success]
        (auto f) mutable {
            if (_stop) return; // Stop processing if requested

            auto result = f.get();
            if (!result) {
                LOG(ERROR) << "StreamToBuffer failed for request " << request_id << ": " << result.status().message();
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
                    LOG(ERROR) << "StreamToBuffer overflow for request " << request_id;
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
                    LOG(ERROR) << "StreamToBuffer incomplete read for request " << request_id 
                               << ". Expected " << remaining_in_chunk << ", got " << bytes_copied;
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

common::ResponseCode AsyncClientGrpc::Read(
    const std::string& path,
    common::backend_api::ObjectRange_t range,
    char* destination_buffer,
    common::backend_api::ObjectRequestId_t request_id,
    std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder)
{
    // We use a single chunk for the whole range in the new async client
    auto pending_chunks = std::make_shared<std::atomic<unsigned>>(1);
    auto is_success = std::make_shared<std::atomic<bool>>(true);

    const auto uri = common::s3::StorageUri(path);
    std::string bucket_name(uri.bucket);
    std::string path_name(uri.path);
    std::string key = path; 

    // Helper to trigger read on a descriptor
    auto trigger_read = [this, range, destination_buffer, request_id, responder, pending_chunks, is_success](
        std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor) {
            auto read_result = descriptor->Read(range.offset, range.length);
            
            StreamToBuffer(std::move(read_result.first), std::move(read_result.second), 
                        destination_buffer, range.length, request_id, responder, pending_chunks, is_success);
    };

    // Optimization: Lock-free check.
    {
        auto it = _descriptors.find(key);
        if (it != _descriptors.end()) {
            trigger_read(it->second);
            return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
        }
    }

    // Fallback to locking mechanism
    {
        // 1. Check Cache (Shared Lock)
        std::shared_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
        auto it = _descriptors.find(key);
        if (it != _descriptors.end()) {
            auto descriptor = it->second;
            lock.unlock();
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
        trigger_read(descriptor);
        return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
    }

    // 2. Check/Add to Pending Opens: Coalesce requests for the same object.
    auto callback = [trigger_read, responder, request_id, is_success](std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor) {
        if (!descriptor) {
            // Open failed, fail this specific request
            if (is_success->exchange(false)) {
                responder->push({request_id, common::ResponseCode::FileAccessError});
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
        return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
    }

    lock.unlock();

    // 3. Initiate Open
    _client->Open(google::cloud::storage_experimental::BucketName(bucket_name), path_name)
        .then([this, key, request_id](auto f) {
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

} // namespace runai::llm::streamer::impl::gcs
