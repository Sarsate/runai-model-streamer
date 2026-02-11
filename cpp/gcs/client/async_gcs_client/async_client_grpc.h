#pragma once

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <atomic>
#include <functional>
#include <shared_mutex>

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

    std::unique_ptr<google::cloud::storage_experimental::AsyncClient> _client;
    
    std::shared_timed_mutex _descriptors_mutex;
    std::map<std::string, std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> _descriptors;
    std::map<std::string, std::vector<std::function<void(std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>)>>> _pending_opens;

    std::atomic<bool> _stop{false};
};

} // namespace runai::llm::streamer::impl::gcs
