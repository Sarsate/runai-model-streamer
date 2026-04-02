#include <grpc/grpc.h>
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
    }, 40)
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
    _thread_pool.stop();

    if (_monitor_thread.joinable()) {
        _monitor_thread.join();
    }
    
    // Wait for all active tasks to finish before returning control to Python.
    // This prevents the segfault caused by Python unmapping the .so file
    // while worker threads are still executing ExecuteTask.
    while (_active_tasks.load() > 0) {
        std::this_thread::yield();
    }
    
    // We purposefully DO NOT call _client.reset() here.
    // The gRPC C-core background threads are noisy and slow to tear down.
    // Let the OS process exit handle the cleanup of the HTTP/2 channels.
}

std::shared_future<std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> 
AsyncClientGrpc::TriggerOpen(const std::string& key, const std::string& bucket, const std::string& path) {
    std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);

    auto it = _descriptors.find(key);
    if (it != _descriptors.end()) {
        it->second.last_used = std::chrono::steady_clock::now();
        // If it's already here, we just return the future.
        // The caller (GetDescriptor) will handle the fast-path avoiding this function entirely when possible.
        return it->second.descriptor_future;
    }

    auto p = std::make_shared<std::promise<std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>>>();
    std::shared_future<std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> fut = p->get_future();
    
    _descriptors[key] = {nullptr, fut, std::chrono::steady_clock::now()};

    _client->Open(google::cloud::storage_experimental::BucketName(bucket), path)
        .then([this, key, p](auto f) {
            auto result = f.get();
            std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
            
            if (!result) {
                LOG(ERROR) << "Failed to open descriptor for " << key << ": " << result.status().message();
                try {
                    std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                    _descriptors.erase(key);
                } catch (...) {}
            } else {
                descriptor = std::make_shared<google::cloud::storage_experimental::ObjectDescriptor>(*std::move(result));
                try {
                    std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                    auto it2 = _descriptors.find(key);
                    if (it2 != _descriptors.end()) {
                        it2->second.descriptor = descriptor;
                    }
                } catch (...) {}
            }

            p->set_value(descriptor);
        });

    return fut;
}

std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> 
AsyncClientGrpc::GetDescriptor(const std::string& key, const std::string& bucket, const std::string& path) {
    // FAST PATH: Read-only lock to check if the raw pointer is already resolved and cached.
    // This avoids any future::get() calls or promise allocations on 99.9% of requests.
    {
        std::shared_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
        auto it = _descriptors.find(key);
        if (it != _descriptors.end()) {
            if (it->second.descriptor) {
                // We cannot update last_used under a shared_lock easily, but since this is hot,
                // the monitor eviction (1 second idle) is extremely unlikely to hit a file 
                // being hammered 50 times a second anyway.
                return it->second.descriptor;
            }
        }
    }

    // SLOW PATH: If not fully resolved, fall back to the coalescing logic 
    // which handles concurrent opens safely using futures.
    return TriggerOpen(key, bucket, path).get();
}

void AsyncClientGrpc::ExecuteTask(ReadTask&& task, std::atomic<bool>& stopped) {
    if (stopped) return;
    _active_tasks++;

    auto finalize = [&](bool success) {
        _active_tasks--;
        if (!success) {
            if (task.is_success->exchange(false)) {
                task.responder->push({task.request_id, common::ResponseCode::FileAccessError});
            }
        } else {
             if (task.pending_chunks->fetch_sub(1) == 1) {
                if (task.is_success->load()) {
                    task.responder->push({task.request_id, common::ResponseCode::Success});
                }
            }
        }
    };

    std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
    
    try {
        descriptor = GetDescriptor(task.key, task.bucket_name, task.path_name);
    } catch (...) {
        finalize(false);
        return;
    }
    
    if (!descriptor) {
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

            if (stopped) {
                finalize(false);
                return;
            }

            auto result = pending_read.get(); 

            if (!result) {
                 LOG(ERROR) << "Read failed for request " << task.request_id << ": " << result.status().message();
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
             finalize(false);
             return;
        }
    } catch (...) {
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
    const char* env_chunk_size = std::getenv("RUNAI_STREAMER_GCS_ASYNC_CHUNK_SIZE");
    const size_t CHUNK_SIZE = env_chunk_size ? std::stoull(env_chunk_size) : 419430400; // Default 400 MiB
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
                
                bool expired = false;
                if (idle_time >= 1) {
                    if (it->second.descriptor) {
                        if (it->second.descriptor.use_count() == 1) {
                            expired = true;
                        }
                    } else {
                        if (it->second.descriptor_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                            auto desc = it->second.descriptor_future.get();
                            if (!desc) expired = true;
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
