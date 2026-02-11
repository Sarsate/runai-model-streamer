#include "gcs/client_configuration/client_configuration.h"

#include "google/cloud/storage/client.h"
#include "google/cloud/grpc_options.h"

#include "common/exception/exception.h"
#include "common/response_code/response_code.h"
#include "utils/logging/logging.h"
#include "utils/env/env.h"

#include <fstream>
#include <thread>
#include <algorithm>
#include <cstdlib>
#include <set>
#include <chrono>
#include <iostream>

namespace runai::llm::streamer::impl::gcs
{

ClientConfiguration::ClientConfiguration()
{
    // std::cerr << "DEBUG: ClientConfiguration constructor called" << std::endl;
    
    // Debug helper to check raw env var
    auto check_env = [](const char* name) {
        const char* val = std::getenv(name);
        if (val) {
            // std::cerr << "DEBUG: Env var " << name << " = '" << val << "'" << std::endl;
        } else {
            // std::cerr << "DEBUG: Env var " << name << " is unset" << std::endl;
        }
    };

    check_env("RUNAI_STREAMER_S3_MAX_CONNECTIONS");
    check_env("RUNAI_STREAMER_CONCURRENCY");
    check_env("RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS");
    check_env("RUNAI_STREAMER_S3_LOW_SPEED_LIMIT");
    check_env("RUNAI_STREAMER_S3_TRACE");
    check_env("RUNAI_STREAMER_GCS_USE_ASYNC_CLIENT");

    const auto max_connections = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_MAX_CONNECTIONS", 0);
    if (max_connections) {
        max_concurrency = max_connections;
    } else {
        unsigned nprocs = std::thread::hardware_concurrency();
        // Use at least 8 threads if hardware_concurrency cannot be computed.
        LOG(SPAM) << "Hardware concurrency detected: " << nprocs;
        unsigned default_max_concurrency = nprocs == 0 ? 8U : 1U;
        unsigned worker_concurrency = utils::getenv<unsigned long>("RUNAI_STREAMER_CONCURRENCY", 8UL);
        LOG(SPAM) << "Streamer worker concurrency: " << worker_concurrency;
        max_concurrency = std::max(default_max_concurrency, nprocs * 2 / worker_concurrency);
    }
    LOG(DEBUG) << "GCS per-client concurrency is set to: " << max_concurrency;

    // if the transfer speed is less than the low speed limit for request_timeout_ms milliseconds the transfer is aborted and retried
    const auto request_timeout_ms = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS", 600000);
    if (request_timeout_ms)
    {
        LOG(DEBUG) << "GCS request timeout is set to " << request_timeout_ms << " ms";
        std::chrono::seconds request_timeout_seconds = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::milliseconds(request_timeout_ms));
        options.set<google::cloud::storage::DownloadStallTimeoutOption>(request_timeout_seconds);
    }

    const auto low_speed_limit = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_LOW_SPEED_LIMIT", 0);
    if (low_speed_limit)
    {
        LOG(DEBUG) << "GCS minimum speed is set to " << low_speed_limit << " bytes in second";
        options.set<google::cloud::storage::DownloadStallMinimumRateOption>(low_speed_limit);
    }

    /*
    const auto trace_gcs = utils::getenv<bool>("RUNAI_STREAMER_S3_TRACE", false);
    if (trace_gcs)
    {
        LOG(DEBUG) << "Enabling log tracing for rpc/auth/http modules for GCS API calls";
        std::set<std::string> logging_components;
        logging_components.insert("rpc");
        logging_components.insert("auth");
        logging_components.insert("http");

        options.set<google::cloud::LoggingComponentsOption>(std::move(logging_components));
    }
    */

    const auto sa_key_file_name = utils::getenv<std::string>("RUNAI_STREAMER_GCS_CREDENTIAL_FILE", "");
    if (!sa_key_file_name.empty()) {
        LOG(DEBUG) << "Loading credentials for Service Account from file: " << sa_key_file_name;
        try {
            // Open the file stream.
            auto is = std::ifstream(sa_key_file_name);

            // Configure the stream to throw an exception on any failure (e.g., file not found, read error).
            is.exceptions(std::ifstream::failbit | std::ifstream::badbit);

            // Read the entire file into the 'contents' string.
            auto contents = std::string(std::istreambuf_iterator<char>(is.rdbuf()), {});

            // Use the contents to set the credentials.
            options.set<google::cloud::UnifiedCredentialsOption>(
                google::cloud::MakeServiceAccountCredentials(contents));
        } catch (const std::ios_base::failure& ex) {
            LOG(ERROR) << "Failed to read service account key file: " << sa_key_file_name;
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
    }

    try {
        use_new_async_client = utils::getenv<bool>("RUNAI_STREAMER_GCS_USE_ASYNC_CLIENT", false);
    } catch (const std::exception& e) {
        // std::cerr << "DEBUG ERROR: Failed to parse RUNAI_STREAMER_GCS_USE_ASYNC_CLIENT: " << e.what() << std::endl;
        // Fallback or rethrow? Let's check the raw value manually to be helpful.
        std::string raw_val = std::getenv("RUNAI_STREAMER_GCS_USE_ASYNC_CLIENT") ? std::getenv("RUNAI_STREAMER_GCS_USE_ASYNC_CLIENT") : "";
        if (raw_val == "true" || raw_val == "True" || raw_val == "TRUE") {
            use_new_async_client = true;
            // std::cerr << "DEBUG: Handled 'true' string manually." << std::endl;
        } else {
             throw; // Rethrow if it's not a simple boolean string mismatch
        }
    }

    if (use_new_async_client) {
        LOG(DEBUG) << "Using new AsyncClient";
        // std::cerr << "DEBUG: ClientConfiguration: RUNAI_STREAMER_GCS_USE_ASYNC_CLIENT is TRUE" << std::endl;

        // Since we are creating multiple GCSClient instances (one per worker), and each
        // now has its own AsyncClient, we should limit the number of channels per AsyncClient
        // to avoid exhausting file descriptors. 1 channel per client * max_concurrency clients
        // results in max_concurrency total channels, which is the intended behavior.
        int num_channels = 1;
        options.set<google::cloud::GrpcNumChannelsOption>(num_channels);
        
        // Limit background threads per client to prevent thread explosion (50 clients * N threads)
        options.set<google::cloud::GrpcBackgroundThreadPoolSizeOption>(1);
        
        // std::cerr << "DEBUG: Setting GrpcNumChannelsOption to " << num_channels << std::endl;
    }
    // std::cerr << "DEBUG: ClientConfiguration constructor finished" << std::endl;
}

}; // namespace runai::llm::streamer::impl::gcs
