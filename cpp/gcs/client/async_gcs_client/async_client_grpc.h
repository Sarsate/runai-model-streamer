#pragma once

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <atomic>
#include <functional>
#include <shared_mutex>
#include <thread>
#include <condition_variable>

#include "google/cloud/storage/async/client.h"
#include "google/cloud/storage/async/object_descriptor.h"

#include "gcs/client_configuration/client_configuration.h"
#include "common/backend_api/object_storage/object_storage.h"
#include "common/backend_api/response/response.h"
#include "common/shared_queue/shared_queue.h"
#include "utils/threadpool/threadpool.h"

namespace runai::llm::streamer::impl::gcs
{

class AsyncClientGrpc
{
public:
    explicit AsyncClientGrpc(const ClientConfiguration& config);
    ~AsyncClientGrpc();

    void PreOpen(const std::vector<std::string>& paths);

    common::ResponseCode Read(
        const std::string& path,
        common::backend_api::ObjectRange_t range,
        char* destination_buffer,
        common::backend_api::ObjectRequestId_t request_id,
        std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder);

    void Stop();

private:
    struct ReadTask {
        std::string bucket_name;
        std::string path_name;
        std::string key;
        size_t offset;
        size_t length;
        char* buffer;
        common::backend_api::ObjectRequestId_t request_id;
        std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder;
        // Shared state for multi-chunk requests (if we split them)
        std::shared_ptr<std::atomic<unsigned>> pending_chunks;
        std::shared_ptr<std::atomic<bool>> is_success;
    };

    void ExecuteTask(ReadTask&& task, std::atomic<bool>& stopped);

    std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> GetDescriptor(const std::string& key, const std::string& bucket, const std::string& path);

    std::shared_ptr<google::cloud::storage_experimental::AsyncClient> _client;
    
    std::shared_timed_mutex _descriptors_mutex;
    std::map<std::string, std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> _descriptors;
    // We might not need pending opens map if we block on future in the thread pool,
    // but PreOpen might still want to populate it asynchronously.
    // For now, let's simplify: GetDescriptor manages the cache.

    std::atomic<bool> _stop{false};
    utils::ThreadPool<ReadTask> _thread_pool;

    // Monitor
    std::thread _monitor_thread;
    std::mutex _monitor_mutex;
    std::condition_variable _monitor_cv;
    void Monitor();
};

} // namespace runai::llm::streamer::impl::gcs
