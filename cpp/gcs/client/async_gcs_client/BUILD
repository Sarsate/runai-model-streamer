load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "async_gcs_client",
    deps = [
        "@google_cloud_cpp//:storage",
        "@google_cloud_cpp//:storage_grpc",
        "//gcs/client_configuration",
        "//common/backend_api/response",
        "//common/shared_queue",
        "//common/storage_uri",
        "//common/backend_api/object_storage",
        "//utils/logging",
        "//utils/threadpool",
    ],
)
