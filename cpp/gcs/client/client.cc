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
    std::cerr << "--- Jetski: GCSClient constructor entered" << std::endl;
    if (_client_config.use_new_async_client) {
        _async_client_grpc = std::make_unique<AsyncClientGrpc>(_client_config);
    } else {
        _client = std::make_unique<AsyncGcsClient>(_client_config.options, _client_config.max_concurrency);
    }
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
        LOG(WARNING) << "Requesting response with uninitialized responder";
        return common::ResponseCode::FinishedError;
    }

    return _responder->pop();
}

common::ResponseCode write_stream_to_buffer(
        google::cloud::storage::ObjectReadStream && stream,
        char * dest_buffer,
        size_t bytesize,
        common::backend_api::ObjectRequestId_t request_id) {
    std::cerr << "--- Jetski: Entering write_stream_to_buffer for request " << request_id << std::endl;
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
        std::cerr << "Read failed. Received " << bytes_received << " of " << bytesize << " bytes. GCS Status: " << stream.status().message() << std::endl;
        return common::ResponseCode::FileAccessError;
    }
    if (stream.bad()) {
        std::cerr << "Stream bad. GCS Status: " << stream.status().message() << std::endl;
        return common::ResponseCode::FileAccessError;
    }

    return common::ResponseCode::Success;
}

GCSClient::~GCSClient() {
}

common::ResponseCode GCSClient::async_read(const char* path, common::backend_api::ObjectRange_t range, char* destination_buffer, common::backend_api::ObjectRequestId_t request_id)
{
    std::cerr << "--- Jetski: GCSClient::async_read called for path: " << path << " request_id: " << request_id << std::endl;

    if (_responder == nullptr)
    {
        _responder = std::make_shared<Responder>(1);
    }
    else
    {
        _responder->increment(1);
    }

    if (_client_config.use_new_async_client && _async_client_grpc) {
        return _async_client_grpc->Read(path, range, destination_buffer, request_id, _responder);
    }

    char * buffer_ = destination_buffer;
    // split range into chunks
    size_t size = std::max(1UL, range.length/_chunk_bytesize);

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
            try {
                auto stream = f.get();
                auto response_code = write_stream_to_buffer(std::move(stream), dest_buffer, bytesize_, request_id);
                if (response_code == common::ResponseCode::Success)
                {
                    const auto running = counter->fetch_sub(1);
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
                    std::cerr << "Chunk read failed for request_id: " << request_id << " error: " << response_code << std::endl;
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
            } catch (const std::exception& e) {
                std::cerr << "Exception in ReadObjectAsync future: " << e.what() << std::endl;
                bool previous = is_success->exchange(false);
                if (previous) {
                    common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
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
    if (_async_client_grpc) {
        _async_client_grpc->Stop();
    }
    if (_responder != nullptr)
    {
        _responder->stop();
    }
}

}; // namespace runai::llm::streamer::impl::gcs
