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
#include <queue>

#include "google/cloud/storage/async/client.h"
#include "google/cloud/storage/async/object_descriptor.h"

#include "gcs/client_configuration/client_configuration.h"
#include "common/backend_api/object_storage/object_storage.h"
#include "common/backend_api/response/response.h"
#include "common/shared_queue/shared_queue.h"

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
    void StreamToBuffer(
        google::cloud::storage_experimental::AsyncReader reader,
        google::cloud::storage_experimental::AsyncToken token,
        char* buffer,
        size_t remaining_in_chunk,
        common::backend_api::ObjectRequestId_t request_id,
        std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder,
        std::shared_ptr<std::atomic<unsigned>> pending_chunks,
        std::shared_ptr<std::atomic<bool>> is_success);

    void OnReadComplete(
        google::cloud::future<google::cloud::StatusOr<std::pair<google::cloud::storage_experimental::ReadPayload, google::cloud::storage_experimental::AsyncToken>>> f,
        google::cloud::storage_experimental::AsyncReader reader,
        char* buffer,
        size_t remaining_in_chunk,
        common::backend_api::ObjectRequestId_t request_id,
        std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder,
        std::shared_ptr<std::atomic<unsigned>> pending_chunks,
        std::shared_ptr<std::atomic<bool>> is_success);

    std::shared_ptr<google::cloud::storage_experimental::AsyncClient> _client;
    
    std::shared_timed_mutex _descriptors_mutex;
    std::map<std::string, std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> _descriptors;
    std::map<std::string, std::vector<std::function<void(std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>)>>> _pending_opens;

    std::atomic<bool> _stop{false};
    std::thread _monitor_thread;
    void Monitor();

    // Debug counters
    std::atomic<int> _active_opens{0};
    std::atomic<int> _active_reads{0};
    std::atomic<int> _active_streams{0};

    // Concurrency Control
    int _max_concurrent_reads{96};
    std::mutex _semaphore_mutex;
    std::condition_variable _semaphore_cv;
    void AcquireReadPermit();
    void ReleaseReadPermit();

    // Internal Request Queue
    struct ReadRequest {
        std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
        size_t offset;
        size_t length;
        char* buffer;
        common::backend_api::ObjectRequestId_t request_id;
        std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder;
        std::shared_ptr<std::atomic<unsigned>> pending_chunks;
        std::shared_ptr<std::atomic<bool>> is_success;
    };

    std::queue<ReadRequest> _request_queue;
    std::mutex _queue_mutex;
    std::condition_variable _queue_cv;
    std::thread _worker_thread;
    void WorkerLoop();

    // Monitor Thread Control
    std::mutex _monitor_mutex;
    std::condition_variable _monitor_cv;

    // Instrumentation
    std::atomic<uint64_t> _total_copy_time_us{0};
    std::atomic<uint64_t> _total_bytes_copied{0};
};

} // namespace runai::llm::streamer::impl::gcs
