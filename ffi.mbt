///|
/// Opaque handle for the Temporal Core runtime.
///
/// Create one with `Runtime::new` and pass it to connection and ephemeral
/// server constructors.
pub type Runtime

///|
/// Opaque handle for a Temporal client connection.
///
/// Create one with `Connection::connect_blocking` or `Connection::connect`.
pub type Connection

///|
/// Opaque handle for a Temporal Core worker.
///
/// Workers poll protobuf tasks from Core and complete them with protobuf
/// completion bytes.
pub type Worker

///|
/// Opaque cancellation token used by raw RPC calls.
pub type CancellationToken

///|
/// Opaque deterministic random source backed by Temporal Core.
pub type Random

///|
/// Opaque handle for an ephemeral Temporal test or development server.
pub type EphemeralServer

///|
priv type CoreResult

///|
#borrow(result)
extern "c" fn core_result_is_ok(result : CoreResult) -> Bool = "temporal_mbt_core_result_is_ok"

///|
#borrow(result)
extern "c" fn core_result_is_shutdown(result : CoreResult) -> Bool = "temporal_mbt_core_result_is_shutdown"

///|
#borrow(result)
extern "c" fn core_result_status_code(result : CoreResult) -> Int = "temporal_mbt_core_result_status_code"

///|
#borrow(result)
extern "c" fn core_result_error_message(result : CoreResult) -> Bytes = "temporal_mbt_core_result_error_message"

///|
#borrow(result)
extern "c" fn core_result_failure_details(result : CoreResult) -> Bytes = "temporal_mbt_core_result_failure_details"

///|
#borrow(result)
extern "c" fn core_result_success_bytes(result : CoreResult) -> Bytes = "temporal_mbt_core_result_success_bytes"

///|
#borrow(result)
extern "c" fn core_result_target(result : CoreResult) -> Bytes = "temporal_mbt_core_result_target"

///|
#borrow(result)
extern "c" fn core_result_take_runtime(result : CoreResult) -> Runtime = "temporal_mbt_core_result_take_runtime"

///|
#borrow(result)
extern "c" fn core_result_take_connection(result : CoreResult) -> Connection = "temporal_mbt_core_result_take_connection"

///|
#borrow(result)
extern "c" fn core_result_take_worker(result : CoreResult) -> Worker = "temporal_mbt_core_result_take_worker"

///|
#borrow(result)
extern "c" fn core_result_take_ephemeral_server(
  result : CoreResult,
) -> EphemeralServer = "temporal_mbt_core_result_take_ephemeral_server"

///|
extern "c" fn runtime_new_ffi(
  worker_heartbeat_interval_millis : UInt64,
) -> CoreResult = "temporal_mbt_runtime_new"

///|
#borrow(runtime, target_url, client_name, client_version, identity, api_key, metadata, binary_metadata)
extern "c" fn connection_connect_ffi(
  runtime : Runtime,
  target_url : Bytes,
  client_name : Bytes,
  client_version : Bytes,
  identity : Bytes,
  api_key : Bytes,
  metadata : Bytes,
  binary_metadata : Bytes,
) -> CoreResult = "temporal_mbt_client_connect"

///|
#borrow(connection, api_key)
extern "c" fn connection_update_api_key_ffi(
  connection : Connection,
  api_key : Bytes,
) -> Unit = "temporal_mbt_client_update_api_key"

///|
#borrow(connection, metadata)
extern "c" fn connection_update_metadata_ffi(
  connection : Connection,
  metadata : Bytes,
) -> Unit = "temporal_mbt_client_update_metadata"

///|
#borrow(connection, metadata)
extern "c" fn connection_update_binary_metadata_ffi(
  connection : Connection,
  metadata : Bytes,
) -> Unit = "temporal_mbt_client_update_binary_metadata"

///|
#borrow(connection, rpc, request, metadata, binary_metadata)
extern "c" fn connection_rpc_call_ffi(
  connection : Connection,
  service : Int,
  rpc : Bytes,
  request : Bytes,
  retry : Bool,
  metadata : Bytes,
  binary_metadata : Bytes,
  timeout_millis : UInt,
) -> CoreResult = "temporal_mbt_client_rpc_call"

///|
#borrow(connection, rpc, request, metadata, binary_metadata, cancellation_token)
extern "c" fn connection_rpc_call_with_cancel_ffi(
  connection : Connection,
  service : Int,
  rpc : Bytes,
  request : Bytes,
  retry : Bool,
  metadata : Bytes,
  binary_metadata : Bytes,
  timeout_millis : UInt,
  cancellation_token : CancellationToken,
) -> CoreResult = "temporal_mbt_client_rpc_call_with_cancel"

///|
extern "c" fn cancellation_token_new_ffi() -> CancellationToken = "temporal_mbt_cancellation_token_new"

///|
#borrow(token)
extern "c" fn cancellation_token_cancel_ffi(token : CancellationToken) -> Unit = "temporal_mbt_cancellation_token_cancel"

///|
extern "c" fn random_new_ffi(seed : UInt64) -> Random = "temporal_mbt_random_new"

///|
#borrow(random)
extern "c" fn random_int32_range_ffi(
  random : Random,
  min : Int,
  max : Int,
  max_inclusive : Bool,
) -> Int = "temporal_mbt_random_int32_range"

///|
#borrow(random)
extern "c" fn random_double_range_ffi(
  random : Random,
  min : Double,
  max : Double,
  max_inclusive : Bool,
) -> Double = "temporal_mbt_random_double_range"

///|
#borrow(random)
extern "c" fn random_fill_bytes_ffi(random : Random, len : Int) -> Bytes = "temporal_mbt_random_fill_bytes"

///|
#borrow(connection, namespace_name, task_queue, build_id, identity_override)
extern "c" fn worker_new_ffi(
  connection : Connection,
  namespace_name : Bytes,
  task_queue : Bytes,
  build_id : Bytes,
  identity_override : Bytes,
  max_cached_workflows : UInt,
  workflow_slots : Int,
  activity_slots : Int,
  local_activity_slots : Int,
  nexus_slots : Int,
  enable_workflows : Bool,
  enable_local_activities : Bool,
  enable_remote_activities : Bool,
  enable_nexus : Bool,
  sticky_queue_schedule_to_start_timeout_millis : UInt64,
  max_heartbeat_throttle_interval_millis : UInt64,
  default_heartbeat_throttle_interval_millis : UInt64,
  max_activities_per_second : Double,
  max_task_queue_activities_per_second : Double,
  graceful_shutdown_period_millis : UInt64,
  workflow_task_poller_maximum : Int,
  nonsticky_to_sticky_poll_ratio : Double,
  activity_task_poller_maximum : Int,
  nexus_task_poller_maximum : Int,
  nondeterminism_as_workflow_fail : Bool,
) -> CoreResult = "temporal_mbt_worker_new"

///|
#borrow(worker)
extern "c" fn worker_validate_ffi(worker : Worker) -> CoreResult = "temporal_mbt_worker_validate"

///|
#borrow(worker, connection)
extern "c" fn worker_replace_client_ffi(
  worker : Worker,
  connection : Connection,
) -> CoreResult = "temporal_mbt_worker_replace_client"

///|
#borrow(worker)
extern "c" fn worker_poll_workflow_activation_ffi(
  worker : Worker,
) -> CoreResult = "temporal_mbt_worker_poll_workflow_activation"

///|
#borrow(worker)
extern "c" fn worker_poll_activity_task_ffi(worker : Worker) -> CoreResult = "temporal_mbt_worker_poll_activity_task"

///|
#borrow(worker)
extern "c" fn worker_poll_nexus_task_ffi(worker : Worker) -> CoreResult = "temporal_mbt_worker_poll_nexus_task"

///|
#borrow(worker, completion)
extern "c" fn worker_complete_workflow_activation_ffi(
  worker : Worker,
  completion : Bytes,
) -> CoreResult = "temporal_mbt_worker_complete_workflow_activation"

///|
#borrow(worker, completion)
extern "c" fn worker_complete_activity_task_ffi(
  worker : Worker,
  completion : Bytes,
) -> CoreResult = "temporal_mbt_worker_complete_activity_task"

///|
#borrow(worker, completion)
extern "c" fn worker_complete_nexus_task_ffi(
  worker : Worker,
  completion : Bytes,
) -> CoreResult = "temporal_mbt_worker_complete_nexus_task"

///|
#borrow(worker, heartbeat)
extern "c" fn worker_record_activity_heartbeat_ffi(
  worker : Worker,
  heartbeat : Bytes,
) -> CoreResult = "temporal_mbt_worker_record_activity_heartbeat"

///|
#borrow(worker, run_id)
extern "c" fn worker_request_workflow_eviction_ffi(
  worker : Worker,
  run_id : Bytes,
) -> Unit = "temporal_mbt_worker_request_workflow_eviction"

///|
#borrow(worker)
extern "c" fn worker_initiate_shutdown_ffi(worker : Worker) -> Unit = "temporal_mbt_worker_initiate_shutdown"

///|
#borrow(worker)
extern "c" fn worker_finalize_shutdown_ffi(worker : Worker) -> CoreResult = "temporal_mbt_worker_finalize_shutdown"

///|
#borrow(runtime, existing_path, sdk_name, sdk_version, download_version, download_dest_dir, extra_args)
extern "c" fn ephemeral_start_test_server_ffi(
  runtime : Runtime,
  existing_path : Bytes,
  sdk_name : Bytes,
  sdk_version : Bytes,
  download_version : Bytes,
  download_dest_dir : Bytes,
  port : Int,
  extra_args : Bytes,
  download_ttl_seconds : UInt64,
) -> CoreResult = "temporal_mbt_ephemeral_start_test_server"

///|
#borrow(runtime, existing_path, sdk_name, sdk_version, download_version, download_dest_dir, extra_args, namespace_name, ip, database_filename, log_format, log_level)
extern "c" fn ephemeral_start_dev_server_ffi(
  runtime : Runtime,
  existing_path : Bytes,
  sdk_name : Bytes,
  sdk_version : Bytes,
  download_version : Bytes,
  download_dest_dir : Bytes,
  port : Int,
  extra_args : Bytes,
  download_ttl_seconds : UInt64,
  namespace_name : Bytes,
  ip : Bytes,
  database_filename : Bytes,
  ui : Bool,
  ui_port : Int,
  log_format : Bytes,
  log_level : Bytes,
) -> CoreResult = "temporal_mbt_ephemeral_start_dev_server"

///|
#borrow(server)
extern "c" fn ephemeral_server_shutdown_ffi(
  server : EphemeralServer,
) -> CoreResult = "temporal_mbt_ephemeral_server_shutdown"
