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
    
    const char* env_retries = std::getenv("RUNAI_STREAMER_GCS_MAX_RETRIES");
    if (env_retries) _max_retries = std::stoul(env_retries);

    const char* env_timeout = std::getenv("RUNAI_STREAMER_GCS_TIMEOUT_SECONDS");
    if (env_timeout) _timeout_seconds = std::stoul(env_timeout);

    const char* env_min_throughput = std::getenv("RUNAI_STREAMER_MIN_THROUGHPUT_MBPS");
    if (env_min_throughput) _min_throughput_mbps = std::stod(env_min_throughput);

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
    _thread_pool.stopped = true;

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
    auto fut = TriggerOpen(key, bucket, path);
    if (fut.wait_for(std::chrono::seconds(_timeout_seconds)) == std::future_status::ready) {
        return fut.get();
    } else {
        LOG(ERROR) << "Timeout waiting for descriptor for " << key;
        throw std::runtime_error("Timeout waiting for descriptor");
    }
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
        auto read_start = std::chrono::steady_clock::now();
        auto pending_read = reader.Read(std::move(token));

        // 2. Read Loop
        
        while (!stopped) {
            // Block until the *current* chunk arrives, with timeout logging
            std::future_status status;
            int timeout_count = 0;
            do {
                status = pending_read.wait_for(std::chrono::milliseconds(100));
                if (status == std::future_status::timeout) {
                    timeout_count++;
                    if (timeout_count > (_timeout_seconds * 10)) {
                        if (task.retry_count < _max_retries) {
                            LOG(WARNING) << "Task " << task.request_id << " timed out. Re-opening stream and retrying (" << task.retry_count + 1 << "/" << _max_retries << ")";
                            {
                                std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                                _descriptors.erase(task.key);
                            }
                            ReadTask retry_task = task;
                            retry_task.retry_count++;
                            _thread_pool.push(std::move(retry_task));
                            _active_tasks--;
                            return;
                        } else {
                            LOG(ERROR) << "Task " << task.request_id << " failed after " << _max_retries << " retries due to timeout.";
                            finalize(false);
                            return;
                        }
                    }
                }
            } while (status == std::future_status::timeout && !stopped);

            if (stopped) {
                finalize(false);
                return;
            }

            auto result = pending_read.get(); 
            auto read_end = std::chrono::steady_clock::now();
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(read_end - read_start).count();

            if (!result) {
                 LOG(ERROR) << "Read failed for request " << task.request_id << ": " << result.status().message();
                 if (task.retry_count < _max_retries) {
                     LOG(WARNING) << "Retrying task " << task.request_id << " due to read error (" << task.retry_count + 1 << "/" << _max_retries << ")";
                     {
                         std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                         _descriptors.erase(task.key);
                     }
                     ReadTask retry_task = task;
                     retry_task.retry_count++;
                     _thread_pool.push(std::move(retry_task));
                     _active_tasks--;
                     return;
                 } else {
                     finalize(false);
                     return;
                 }
            }

            auto& payload = result->first;
            auto next_token = std::move(result->second);

            bool has_more = next_token.valid();

            size_t payload_size = 0;
            for (const auto& chunk : payload.contents()) {
                if (bytes_copied + chunk.size() > task.length) {
                    // Overflow
                    finalize(false);
                    return;
                }
                
                // Use non-temporal memcpy optimization
                std::memcpy(task.buffer + bytes_copied, chunk.data(), chunk.size());
                
                bytes_copied += chunk.size();
                payload_size += chunk.size();
            }

            // Update descriptor metrics
            {
                std::shared_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                auto it = _descriptors.find(task.key);
                if (it != _descriptors.end()) {
                    it->second.total_bytes_read->fetch_add(payload_size);
                    it->second.total_time_spent_ms->fetch_add(elapsed_ms);
                }
            }

            if (!has_more) break;

            if (!stopped) {
                read_start = std::chrono::steady_clock::now();
                pending_read = reader.Read(std::move(next_token));
            }
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

                // Slow stream detection
                if (!expired && it->second.descriptor) {
                    auto bytes = it->second.total_bytes_read->load();
                    auto ms = it->second.total_time_spent_ms->load();
                    
                    if (ms > 1000 && bytes > 10 * 1024 * 1024) { // Only check after 1 second and 10MB read
                        double throughput_mbps = (bytes / (1024.0 * 1024.0)) / (ms / 1000.0);
                        if (throughput_mbps < _min_throughput_mbps) {
                            LOG(WARNING) << "Descriptor for " << it->first << " is too slow (" << throughput_mbps << " MB/s). Invalidating.";
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
