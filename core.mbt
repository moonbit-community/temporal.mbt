///|
fn utf8(s : String) -> Bytes {
  @utf8.encode(s)
}

///|
fn utf8_lossy(bytes : Bytes) -> String {
  @utf8.decode_lossy(bytes)
}

///|
fn temporal_error(result : CoreResult) -> TemporalError {
  { message: utf8_lossy(core_result_error_message(result)) }
}

///|
fn unit_result(result : CoreResult) -> Result[Unit, TemporalError] {
  if core_result_is_ok(result) {
    Ok(())
  } else {
    Err(temporal_error(result))
  }
}

///|
/// Creates a Temporal Core runtime.
///
/// The runtime owns Core background resources used by connections, workers, and
/// ephemeral servers. Keep it alive for as long as those handles are in use.
pub fn Runtime::new(
  options? : RuntimeOptions = RuntimeOptions::default(),
) -> Result[Runtime, TemporalError] {
  let result = runtime_new_ffi(options.worker_heartbeat_interval_millis)
  if core_result_is_ok(result) {
    Ok(core_result_take_runtime(result))
  } else {
    Err(temporal_error(result))
  }
}

///|
/// Connects to a Temporal endpoint and waits for Core to finish connection
/// setup.
pub fn Connection::connect_blocking(
  runtime : Runtime,
  options : ConnectionOptions,
) -> Result[Connection, TemporalError] {
  let result = connection_connect_ffi(
    runtime,
    utf8(options.target_url),
    utf8(options.client_name),
    utf8(options.client_version),
    utf8(options.identity),
    utf8(options.api_key),
    options.metadata,
    options.binary_metadata,
  )
  if core_result_is_ok(result) {
    Ok(core_result_take_connection(result))
  } else {
    Err(temporal_error(result))
  }
}

///|
/// Callback-shaped connection helper.
///
/// This currently invokes `connect_blocking` and calls `callback` before
/// returning.
pub fn Connection::connect(
  runtime : Runtime,
  options : ConnectionOptions,
  callback : (Result[Connection, TemporalError]) -> Unit,
) -> Unit {
  callback(Connection::connect_blocking(runtime, options))
}

///|
/// Replaces the API key used by this connection for later calls.
pub fn Connection::update_api_key(self : Connection, api_key : String) -> Unit {
  connection_update_api_key_ffi(self, utf8(api_key))
}

///|
/// Replaces textual gRPC metadata for later calls.
///
/// The byte format is newline-separated key/value pairs.
pub fn Connection::update_metadata(self : Connection, metadata : Bytes) -> Unit {
  connection_update_metadata_ffi(self, metadata)
}

///|
/// Replaces binary gRPC metadata for later calls.
///
/// The byte format is newline-separated key/value pairs.
pub fn Connection::update_binary_metadata(
  self : Connection,
  metadata : Bytes,
) -> Unit {
  connection_update_binary_metadata_ffi(self, metadata)
}

///|
/// Performs a raw Temporal RPC call and waits for the encoded protobuf
/// response.
pub fn Connection::rpc_call_blocking(
  self : Connection,
  options : RpcCallOptions,
) -> Result[Bytes, RpcFailure] {
  let rpc = utf8(options.rpc)
  let result = match options.cancellation_token {
    Some(token) =>
      connection_rpc_call_with_cancel_ffi(
        self,
        options.service.to_int(),
        rpc,
        options.request,
        options.retry,
        options.metadata,
        options.binary_metadata,
        options.timeout_millis,
        token,
      )
    None =>
      connection_rpc_call_ffi(
        self,
        options.service.to_int(),
        rpc,
        options.request,
        options.retry,
        options.metadata,
        options.binary_metadata,
        options.timeout_millis,
      )
  }
  if core_result_is_ok(result) {
    Ok(core_result_success_bytes(result))
  } else {
    Err({
      status_code: core_result_status_code(result),
      message: utf8_lossy(core_result_error_message(result)),
      details: core_result_failure_details(result),
    })
  }
}

///|
/// Callback-shaped raw RPC helper.
///
/// This currently invokes `rpc_call_blocking` and calls `callback` before
/// returning.
pub fn Connection::rpc_call(
  self : Connection,
  options : RpcCallOptions,
  callback : (Result[Bytes, RpcFailure]) -> Unit,
) -> Unit {
  callback(self.rpc_call_blocking(options))
}

///|
/// Creates a cancellation token that can be attached to `RpcCallOptions`.
pub fn CancellationToken::new() -> CancellationToken {
  cancellation_token_new_ffi()
}

///|
/// Requests cancellation for operations using this token.
pub fn CancellationToken::cancel(self : CancellationToken) -> Unit {
  cancellation_token_cancel_ffi(self)
}

///|
/// Creates a deterministic random source using `seed`.
pub fn Random::new(seed : UInt64) -> Random {
  random_new_ffi(seed)
}

///|
/// Returns a deterministic integer in the configured range.
///
/// With the default `max_inclusive = false`, the range is `[min, max)`.
pub fn Random::int32_range(
  self : Random,
  min : Int,
  max : Int,
  max_inclusive? : Bool = false,
) -> Int {
  random_int32_range_ffi(self, min, max, max_inclusive)
}

///|
/// Returns a deterministic floating-point value in the configured range.
///
/// With the default `max_inclusive = false`, the range is `[min, max)`.
pub fn Random::double_range(
  self : Random,
  min : Double,
  max : Double,
  max_inclusive? : Bool = false,
) -> Double {
  random_double_range_ffi(self, min, max, max_inclusive)
}

///|
/// Returns `len` deterministic random bytes.
pub fn Random::fill_bytes(self : Random, len : Int) -> Bytes {
  random_fill_bytes_ffi(self, len)
}

///|
/// Creates a Core worker bound to `connection`.
pub fn Worker::new(
  connection : Connection,
  options : WorkerOptions,
) -> Result[Worker, TemporalError] {
  let task_types = options.task_types
  let result = worker_new_ffi(
    connection,
    utf8(options.namespace_name),
    utf8(options.task_queue),
    utf8(options.build_id),
    utf8(options.identity_override),
    options.max_cached_workflows,
    options.workflow_slots,
    options.activity_slots,
    options.local_activity_slots,
    options.nexus_slots,
    task_types.enable_workflows,
    task_types.enable_local_activities,
    task_types.enable_remote_activities,
    task_types.enable_nexus,
    options.sticky_queue_schedule_to_start_timeout_millis,
    options.max_heartbeat_throttle_interval_millis,
    options.default_heartbeat_throttle_interval_millis,
    options.max_activities_per_second,
    options.max_task_queue_activities_per_second,
    options.graceful_shutdown_period_millis,
    options.workflow_task_poller_behavior.simple_maximum,
    options.nonsticky_to_sticky_poll_ratio,
    options.activity_task_poller_behavior.simple_maximum,
    options.nexus_task_poller_behavior.simple_maximum,
    options.nondeterminism_as_workflow_fail,
  )
  if core_result_is_ok(result) {
    Ok(core_result_take_worker(result))
  } else {
    Err(temporal_error(result))
  }
}

///|
/// Validates the worker configuration with Temporal Core.
pub fn Worker::validate_blocking(self : Worker) -> Result[Unit, TemporalError] {
  unit_result(worker_validate_ffi(self))
}

///|
/// Callback-shaped worker validation helper.
///
/// This currently invokes `validate_blocking` and calls `callback` before
/// returning.
pub fn Worker::validate(
  self : Worker,
  callback : (Result[Unit, TemporalError]) -> Unit,
) -> Unit {
  callback(self.validate_blocking())
}

///|
/// Replaces the client connection used by this worker.
pub fn Worker::replace_client(
  self : Worker,
  connection : Connection,
) -> Result[Unit, TemporalError] {
  unit_result(worker_replace_client_ffi(self, connection))
}

///|
/// Polls for the next workflow activation.
///
/// Returns `Ok(None)` when the worker has shut down. Otherwise the returned
/// bytes are an encoded Core workflow activation protobuf.
pub fn Worker::poll_workflow_activation_blocking(
  self : Worker,
) -> Result[Bytes?, TemporalError] {
  let result = worker_poll_workflow_activation_ffi(self)
  if core_result_is_ok(result) {
    if core_result_is_shutdown(result) {
      Ok(None)
    } else {
      Ok(Some(core_result_success_bytes(result)))
    }
  } else {
    Err(temporal_error(result))
  }
}

///|
/// Callback-shaped workflow activation poll helper.
///
/// This currently invokes `poll_workflow_activation_blocking` and calls
/// `callback` before returning.
pub fn Worker::poll_workflow_activation(
  self : Worker,
  callback : (Result[Bytes?, TemporalError]) -> Unit,
) -> Unit {
  callback(self.poll_workflow_activation_blocking())
}

///|
/// Polls for the next activity task.
///
/// Returns `Ok(None)` when the worker has shut down. Otherwise the returned
/// bytes are an encoded Core activity task protobuf.
pub fn Worker::poll_activity_task_blocking(
  self : Worker,
) -> Result[Bytes?, TemporalError] {
  let result = worker_poll_activity_task_ffi(self)
  if core_result_is_ok(result) {
    if core_result_is_shutdown(result) {
      Ok(None)
    } else {
      Ok(Some(core_result_success_bytes(result)))
    }
  } else {
    Err(temporal_error(result))
  }
}

///|
/// Callback-shaped activity task poll helper.
///
/// This currently invokes `poll_activity_task_blocking` and calls `callback`
/// before returning.
pub fn Worker::poll_activity_task(
  self : Worker,
  callback : (Result[Bytes?, TemporalError]) -> Unit,
) -> Unit {
  callback(self.poll_activity_task_blocking())
}

///|
/// Polls for the next Nexus task.
///
/// Returns `Ok(None)` when the worker has shut down. Otherwise the returned
/// bytes are an encoded Core Nexus task protobuf.
pub fn Worker::poll_nexus_task_blocking(
  self : Worker,
) -> Result[Bytes?, TemporalError] {
  let result = worker_poll_nexus_task_ffi(self)
  if core_result_is_ok(result) {
    if core_result_is_shutdown(result) {
      Ok(None)
    } else {
      Ok(Some(core_result_success_bytes(result)))
    }
  } else {
    Err(temporal_error(result))
  }
}

///|
/// Callback-shaped Nexus task poll helper.
///
/// This currently invokes `poll_nexus_task_blocking` and calls `callback`
/// before returning.
pub fn Worker::poll_nexus_task(
  self : Worker,
  callback : (Result[Bytes?, TemporalError]) -> Unit,
) -> Unit {
  callback(self.poll_nexus_task_blocking())
}

///|
/// Completes a workflow activation with an encoded Core completion protobuf.
pub fn Worker::complete_workflow_activation_blocking(
  self : Worker,
  completion : Bytes,
) -> Result[Unit, TemporalError] {
  unit_result(worker_complete_workflow_activation_ffi(self, completion))
}

///|
/// Callback-shaped workflow activation completion helper.
///
/// This currently invokes `complete_workflow_activation_blocking` and calls
/// `callback` before returning.
pub fn Worker::complete_workflow_activation(
  self : Worker,
  completion : Bytes,
  callback : (Result[Unit, TemporalError]) -> Unit,
) -> Unit {
  callback(self.complete_workflow_activation_blocking(completion))
}

///|
/// Completes an activity task with an encoded Core completion protobuf.
pub fn Worker::complete_activity_task_blocking(
  self : Worker,
  completion : Bytes,
) -> Result[Unit, TemporalError] {
  unit_result(worker_complete_activity_task_ffi(self, completion))
}

///|
/// Callback-shaped activity task completion helper.
///
/// This currently invokes `complete_activity_task_blocking` and calls
/// `callback` before returning.
pub fn Worker::complete_activity_task(
  self : Worker,
  completion : Bytes,
  callback : (Result[Unit, TemporalError]) -> Unit,
) -> Unit {
  callback(self.complete_activity_task_blocking(completion))
}

///|
/// Completes a Nexus task with an encoded Core completion protobuf.
pub fn Worker::complete_nexus_task_blocking(
  self : Worker,
  completion : Bytes,
) -> Result[Unit, TemporalError] {
  unit_result(worker_complete_nexus_task_ffi(self, completion))
}

///|
/// Callback-shaped Nexus task completion helper.
///
/// This currently invokes `complete_nexus_task_blocking` and calls `callback`
/// before returning.
pub fn Worker::complete_nexus_task(
  self : Worker,
  completion : Bytes,
  callback : (Result[Unit, TemporalError]) -> Unit,
) -> Unit {
  callback(self.complete_nexus_task_blocking(completion))
}

///|
/// Records an activity heartbeat using an encoded Core heartbeat protobuf.
pub fn Worker::record_activity_heartbeat(
  self : Worker,
  heartbeat : Bytes,
) -> Result[Unit, TemporalError] {
  unit_result(worker_record_activity_heartbeat_ffi(self, heartbeat))
}

///|
/// Requests eviction for a workflow run id from the worker cache.
pub fn Worker::request_workflow_eviction(
  self : Worker,
  run_id : String,
) -> Unit {
  worker_request_workflow_eviction_ffi(self, utf8(run_id))
}

///|
/// Starts graceful worker shutdown.
pub fn Worker::initiate_shutdown(self : Worker) -> Unit {
  worker_initiate_shutdown_ffi(self)
}

///|
/// Waits for worker shutdown to finish.
pub fn Worker::finalize_shutdown_blocking(
  self : Worker,
) -> Result[Unit, TemporalError] {
  unit_result(worker_finalize_shutdown_ffi(self))
}

///|
/// Callback-shaped shutdown finalization helper.
///
/// This currently invokes `finalize_shutdown_blocking` and calls `callback`
/// before returning.
pub fn Worker::finalize_shutdown(
  self : Worker,
  callback : (Result[Unit, TemporalError]) -> Unit,
) -> Unit {
  callback(self.finalize_shutdown_blocking())
}

///|
/// Starts an ephemeral Temporal test server and returns its target address.
pub fn EphemeralServer::start_test_server_blocking(
  runtime : Runtime,
  options? : TestServerOptions = TestServerOptions::default(),
) -> Result[(EphemeralServer, String), TemporalError] {
  let result = ephemeral_start_test_server_ffi(
    runtime,
    utf8(options.existing_path),
    utf8(options.sdk_name),
    utf8(options.sdk_version),
    utf8(options.download_version),
    utf8(options.download_dest_dir),
    options.port,
    utf8(options.extra_args),
    options.download_ttl_seconds,
  )
  if core_result_is_ok(result) {
    Ok(
      (
        core_result_take_ephemeral_server(result),
        utf8_lossy(core_result_target(result)),
      ),
    )
  } else {
    Err(temporal_error(result))
  }
}

///|
/// Callback-shaped test server startup helper.
///
/// This currently invokes `start_test_server_blocking` and calls `callback`
/// before returning.
pub fn EphemeralServer::start_test_server(
  runtime : Runtime,
  options : TestServerOptions,
  callback : (Result[(EphemeralServer, String), TemporalError]) -> Unit,
) -> Unit {
  callback(EphemeralServer::start_test_server_blocking(runtime, options~))
}

///|
/// Starts an ephemeral Temporal development server and returns its target
/// address.
pub fn EphemeralServer::start_dev_server_blocking(
  runtime : Runtime,
  options? : DevServerOptions = DevServerOptions::default(),
) -> Result[(EphemeralServer, String), TemporalError] {
  let test_server = options.test_server
  let result = ephemeral_start_dev_server_ffi(
    runtime,
    utf8(test_server.existing_path),
    utf8(test_server.sdk_name),
    utf8(test_server.sdk_version),
    utf8(test_server.download_version),
    utf8(test_server.download_dest_dir),
    test_server.port,
    utf8(test_server.extra_args),
    test_server.download_ttl_seconds,
    utf8(options.namespace_name),
    utf8(options.ip),
    utf8(options.database_filename),
    options.ui,
    options.ui_port,
    utf8(options.log_format),
    utf8(options.log_level),
  )
  if core_result_is_ok(result) {
    Ok(
      (
        core_result_take_ephemeral_server(result),
        utf8_lossy(core_result_target(result)),
      ),
    )
  } else {
    Err(temporal_error(result))
  }
}

///|
/// Callback-shaped development server startup helper.
///
/// This currently invokes `start_dev_server_blocking` and calls `callback`
/// before returning.
pub fn EphemeralServer::start_dev_server(
  runtime : Runtime,
  options : DevServerOptions,
  callback : (Result[(EphemeralServer, String), TemporalError]) -> Unit,
) -> Unit {
  callback(EphemeralServer::start_dev_server_blocking(runtime, options~))
}

///|
/// Shuts down an ephemeral server and waits for it to finish.
pub fn EphemeralServer::shutdown_blocking(
  self : EphemeralServer,
) -> Result[Unit, TemporalError] {
  unit_result(ephemeral_server_shutdown_ffi(self))
}

///|
/// Callback-shaped ephemeral server shutdown helper.
///
/// This currently invokes `shutdown_blocking` and calls `callback` before
/// returning.
pub fn EphemeralServer::shutdown(
  self : EphemeralServer,
  callback : (Result[Unit, TemporalError]) -> Unit,
) -> Unit {
  callback(self.shutdown_blocking())
}
