#include "gcs/client/async_gcs_client/async_client_grpc.h"

#include <iostream>
#include <cstring>
#include <thread>
#include <chrono>
#include <algorithm>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

#include "google/cloud/future.h"
#include "google/cloud/storage/client.h"

#include "common/storage_uri/storage_uri.h"
#include "utils/logging/logging.h"

namespace {
// Optimized memcpy using non-temporal stores to bypass cache for large writes.
// This significantly improves memory bandwidth when writing to destination buffers
// that will not be immediately read by the CPU (e.g., GPU transfer buffers).

#if defined(__x86_64__) || defined(_M_X64)
__attribute__((target("avx2")))
void non_temporal_memcpy(void* dest, const void* src, size_t n) {
    // Threshold for using NT stores (avoid overhead for tiny copies)
    // 256 bytes is a reasonable cut-off for AVX overhead.
    if (n < 256) {
        std::memcpy(dest, src, n);
        return;
    }

    char* d = static_cast<char*>(dest);
    const char* s = static_cast<const char*>(src);

    // Align destination to 32-byte boundary for AVX
    size_t align_offset = (32 - (reinterpret_cast<uintptr_t>(d) & 31)) & 31;
    if (align_offset > 0) {
        if (n < align_offset) { // Should be covered by < 256 check, but safe
            std::memcpy(d, s, n);
            return;
        }
        std::memcpy(d, s, align_offset);
        d += align_offset;
        s += align_offset;
        n -= align_offset;
    }

    // Main loop: 32 bytes at a time using AVX streaming stores
    // _mm256_stream_si256 requires 32-byte alignment for destination
    size_t blocks = n / 32;
    for (size_t i = 0; i < blocks; ++i) {
        __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(s));
        _mm256_stream_si256(reinterpret_cast<__m256i*>(d), data);
        s += 32;
        d += 32;
    }
    
    // Tail copy
    size_t remaining = n % 32;
    if (remaining > 0) {
        std::memcpy(d, s, remaining);
    }
    
    // SFENCE is usually needed after NT stores to ensure visibility before
    // another thread consumes data.
    _mm_sfence();
}
#else
// Fallback for non-x86 architectures (e.g., ARM64)
void non_temporal_memcpy(void* dest, const void* src, size_t n) {
    std::memcpy(dest, src, n);
}
#endif
} // namespace

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

void AsyncClientGrpc::PreOpen(const std::vector<std::string>& paths)
{
    // Fire-and-forget TriggerOpen to populate cache/pending map.
    // This allows subsequent Reads to coalesce onto these operations.
    for (const auto& path : paths) {
        const auto uri = common::s3::StorageUri(path);
        TriggerOpen(path, std::string(uri.bucket), std::string(uri.path));
    }
}

std::shared_future<std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> 
AsyncClientGrpc::TriggerOpen(const std::string& key, const std::string& bucket, const std::string& path) {
    std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);

    // 1. Check Cache
    auto it = _descriptors.find(key);
    if (it != _descriptors.end()) {
        std::promise<std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> p;
        p.set_value(it->second);
        return p.get_future();
    }

    // 2. Check Pending Coalesced Opens
    auto pending_it = _pending_opens.find(key);
    if (pending_it != _pending_opens.end()) {
        return pending_it->second;
    }

    // 3. Initiate New Open
    auto p = std::make_shared<std::promise<std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>>>();
    std::shared_future<std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> fut = p->get_future();
    _pending_opens[key] = fut;

    // We can hold the lock while firing Open because Open returns a future instantly.
    // It does NOT block.
    _client->Open(google::cloud::storage_experimental::BucketName(bucket), path)
        .then([this, key, p](auto f) {
            auto result = f.get();
            std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
            
            if (!result) {
                LOG(ERROR) << "Failed to open descriptor for " << key << ": " << result.status().message();
            } else {
                descriptor = std::make_shared<google::cloud::storage_experimental::ObjectDescriptor>(*std::move(result));
            }

            try {
                std::unique_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
                if (descriptor) {
                    _descriptors[key] = descriptor;
                }
                _pending_opens.erase(key);
            } catch (...) {
                LOG(WARNING) << "Exception in TriggerOpen callback (likely shutdown race)";
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
                status = pending_read.wait_for(std::chrono::seconds(10));
                if (status == std::future_status::timeout) {
                    LOG(WARNING) << "Request " << task.request_id << " timed out waiting for chunk from GCS. Retrying wait...";
                }
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
                non_temporal_memcpy(task.buffer + bytes_copied, chunk.data(), chunk.size());
                
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
        size_t pending_opens_count = 0;
        {
            std::shared_lock<std::shared_timed_mutex> lock(_descriptors_mutex);
            descriptors_count = _descriptors.size();
            pending_opens_count = _pending_opens.size();
        }
        
        LOG(INFO) << "AsyncClientGrpc Monitor: "
                    << "Active Tasks: " << _active_tasks.load() << ", "
                    << "Cached Descriptors: " << descriptors_count << ", "
                    << "Pending Coalesced Opens: " << pending_opens_count;
        
        std::unique_lock<std::mutex> lock(_monitor_mutex);
        _monitor_cv.wait_for(lock, std::chrono::seconds(2), [this] { return _stop.load(); });
    }
}

} // namespace runai::llm::streamer::impl::gcs
