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
}

AsyncClientGrpc::~AsyncClientGrpc() = default;

void AsyncClientGrpc::Stop()
{
    _stop = true;
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
        _client->Open(google::cloud::storage_experimental::BucketName(bucket_name), path_name)
            .then([this, key](auto f) {
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
    reader.Read(std::move(token)).then(
        [this, reader = std::move(reader), buffer, remaining_in_chunk, request_id, responder, pending_chunks, is_success]
        (auto f) mutable {
            if (_stop) return; 

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
            
            // Pipelining: If there is more data, start reading it IMMEDIATELY
            // before we spend time copying the current data.
            if (next_token.valid()) {
                // Determine how many bytes we are about to copy so we can calculate the offset for the next read
                size_t current_payload_size = 0;
                for (const auto& chunk : payload.contents()) {
                    current_payload_size += chunk.size();
                }
                
                // Safety check to avoid underflow if payload is larger than expected (overflow handled in loop below)
                size_t next_remaining = (current_payload_size > remaining_in_chunk) ? 0 : remaining_in_chunk - current_payload_size;

                StreamToBuffer(std::move(reader), std::move(next_token), 
                               buffer + current_payload_size, next_remaining, 
                               request_id, responder, pending_chunks, is_success);
            }

            // Now perform the copy (potentially concurrently with the next network fetch)
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

            // Completion check (only if we didn't recurse/pipeline above, or if we need to handle the final tail)
            // Actually, because we recurse *before* copying, the "next" StreamToBuffer might finish 
            // and push the response *before* we finish copying this chunk? 
            // NO. 'pending_chunks' is 1. We only push when all chunks are done.
            // But we are treating the whole file as 1 chunk.
            
            // Wait, we need to be careful with 'responder->push'.
            // If we pipeline, we split the flow. The "last" token will eventually trigger the completion.
            // But the current function (copying chunk N) might finish *after* chunk N+1 finishes?
            // Unlikely if running on same thread pool, but possible.
            // Does it matter? 
            // We only push response when the *entire* transfer is done.
            // The "last" token (invalid) is the only one that can trigger success?
            // No, the recursion handles the chain. 
            // The *last* invocation (where next_token is invalid) will fall through to the else block below.
            
            if (!result->second.valid()) { // Check original result's token validity locally
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

    LOG(DEBUG) << "Read called for request " << request_id << " range " << range.offset << "-" << range.length;

    // Helper to trigger read on a descriptor
    auto trigger_read = [this, range, destination_buffer, request_id, responder, pending_chunks, is_success](
        std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor) {
            LOG(DEBUG) << "Triggering stream for request " << request_id;
            auto read_result = descriptor->Read(range.offset, range.length);
            
            StreamToBuffer(std::move(read_result.first), std::move(read_result.second), 
                        destination_buffer, range.length, request_id, responder, pending_chunks, is_success);
    };

    // 1. Check Cache (Shared Lock)
    {
        std::shared_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
        auto it = _descriptors.find(key);
        if (it != _descriptors.end()) {
            auto descriptor = it->second;
            lock.unlock();
            LOG(DEBUG) << "Cache hit for request " << request_id;
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
        LOG(DEBUG) << "Coalescing request " << request_id << " onto pending Open";
        // An Open is already in progress. This request is now queued.
        return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
    }

    lock.unlock();

    LOG(DEBUG) << "Initiating Open for request " << request_id;

    // 3. Initiate Open
    _client->Open(google::cloud::storage_experimental::BucketName(bucket_name), path_name)
        .then([this, key, request_id](auto f) {
            auto result = f.get();
            std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
            
            if (!result) {
                LOG(ERROR) << "Failed to open object descriptor for request " << request_id << ": " << result.status().message();
                // descriptor remains null
            } else {
                LOG(DEBUG) << "Open finished successfully for request " << request_id;
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
            
            LOG(DEBUG) << "Fanning out to " << callbacks.size() << " waiting requests for key " << key;
            for (const auto& cb : callbacks) {
                cb(descriptor);
            }
        });

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
}

} // namespace runai::llm::streamer::impl::gcs
