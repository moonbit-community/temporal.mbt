///|
/// Error returned by runtime, connection, worker, and ephemeral server setup
/// operations.
///
/// The message is decoded from the Temporal Core bridge error bytes with lossy
/// UTF-8 conversion.
pub(all) struct TemporalError {
  message : String
} derive(Eq, Debug)

///|
/// Failure returned by a raw Temporal RPC call.
///
/// `status_code` is the gRPC status code reported by Core, `message` is a
/// lossy UTF-8 error string, and `details` contains any encoded failure details
/// provided by the bridge.
pub(all) struct RpcFailure {
  status_code : Int
  message : String
  details : Bytes
} derive(Eq, Debug)

///|
/// Runtime construction options.
///
/// `worker_heartbeat_interval_millis = 0` lets Temporal Core use its default
/// worker heartbeat interval.
pub(all) struct RuntimeOptions {
  worker_heartbeat_interval_millis : UInt64
} derive(Eq, Debug)

///|
/// Returns runtime options suitable for normal clients and workers.
pub fn RuntimeOptions::default() -> RuntimeOptions {
  { worker_heartbeat_interval_millis: 0UL }
}

///|
/// Connection options for a Temporal service endpoint.
///
/// Metadata fields are encoded as newline-separated key/value pairs:
/// `key\nvalue\nkey\nvalue`. Keys and values should not contain newline bytes.
pub(all) struct ConnectionOptions {
  target_url : String
  client_name : String
  client_version : String
  identity : String
  api_key : String
  metadata : Bytes
  binary_metadata : Bytes
} derive(Eq, Debug)

///|
/// Creates connection options with defaults for optional client identity fields.
///
/// `target_url` is the Temporal frontend address, for example
/// `http://127.0.0.1:7233`.
pub fn ConnectionOptions::new(
  target_url : String,
  client_name? : String = "temporal.mbt",
  client_version? : String = "0.0.1",
  identity? : String = "",
  api_key? : String = "",
  metadata? : Bytes = b"",
  binary_metadata? : Bytes = b"",
) -> ConnectionOptions {
  {
    target_url,
    client_name,
    client_version,
    identity,
    api_key,
    metadata,
    binary_metadata,
  }
}

///|
/// Temporal service namespace for a raw RPC call.
///
/// The `rpc` field in `RpcCallOptions` names the method within this service,
/// for example `GetSystemInfo` on `Workflow`.
#warnings("-unused_constructor")
pub enum RpcService {
  Workflow
  Operator
  Cloud
  Test
  Health
} derive(Eq, Debug)

///|
fn RpcService::to_int(self : RpcService) -> Int {
  match self {
    Workflow => 1
    Operator => 2
    Cloud => 3
    Test => 4
    Health => 5
  }
}

///|
/// Raw RPC call options.
///
/// `request` is the encoded protobuf request body and the response is returned
/// as encoded protobuf bytes. `timeout_millis = 0` leaves timeout selection to
/// Temporal Core.
pub(all) struct RpcCallOptions {
  service : RpcService
  rpc : String
  request : Bytes
  retry : Bool
  metadata : Bytes
  binary_metadata : Bytes
  timeout_millis : UInt
  cancellation_token : CancellationToken?
}

///|
/// Creates raw RPC call options with retry enabled and an empty protobuf
/// request by default.
pub fn RpcCallOptions::new(
  service : RpcService,
  rpc : String,
  request? : Bytes = b"",
  retry? : Bool = true,
  metadata? : Bytes = b"",
  binary_metadata? : Bytes = b"",
  timeout_millis? : UInt = 0U,
  cancellation_token? : CancellationToken,
) -> RpcCallOptions {
  {
    service,
    rpc,
    request,
    retry,
    metadata,
    binary_metadata,
    timeout_millis,
    cancellation_token,
  }
}

///|
/// Selects which task kinds a worker is allowed to poll and complete.
pub(all) struct WorkerTaskTypes {
  enable_workflows : Bool
  enable_local_activities : Bool
  enable_remote_activities : Bool
  enable_nexus : Bool
} derive(Eq, Debug)

///|
/// Enables all worker task kinds.
pub fn WorkerTaskTypes::default() -> WorkerTaskTypes {
  {
    enable_workflows: true,
    enable_local_activities: true,
    enable_remote_activities: true,
    enable_nexus: true,
  }
}

///|
/// Poller concurrency behavior.
///
/// The current binding exposes the simple maximum-concurrency strategy used by
/// the underlying Core bridge.
pub(all) struct PollerBehavior {
  simple_maximum : Int
} derive(Eq, Debug)

///|
/// Creates a simple poller behavior with at most `maximum` concurrent polls.
pub fn PollerBehavior::simple_maximum(maximum : Int) -> PollerBehavior {
  { simple_maximum: maximum }
}

///|
/// Worker configuration passed to Temporal Core.
///
/// The task queue and namespace identify where the worker polls. Slot counts
/// and poller behavior limit concurrency for each task type. Durations are in
/// milliseconds.
pub(all) struct WorkerOptions {
  namespace_name : String
  task_queue : String
  build_id : String
  identity_override : String
  max_cached_workflows : UInt
  workflow_slots : Int
  activity_slots : Int
  local_activity_slots : Int
  nexus_slots : Int
  task_types : WorkerTaskTypes
  sticky_queue_schedule_to_start_timeout_millis : UInt64
  max_heartbeat_throttle_interval_millis : UInt64
  default_heartbeat_throttle_interval_millis : UInt64
  max_activities_per_second : Double
  max_task_queue_activities_per_second : Double
  graceful_shutdown_period_millis : UInt64
  workflow_task_poller_behavior : PollerBehavior
  nonsticky_to_sticky_poll_ratio : Double
  activity_task_poller_behavior : PollerBehavior
  nexus_task_poller_behavior : PollerBehavior
  nondeterminism_as_workflow_fail : Bool
}

///|
/// Creates worker options with conservative Core defaults.
pub fn WorkerOptions::new(
  namespace_name : String,
  task_queue : String,
  build_id? : String = "",
  identity_override? : String = "",
  max_cached_workflows? : UInt = 1000U,
  workflow_slots? : Int = 100,
  activity_slots? : Int = 100,
  local_activity_slots? : Int = 100,
  nexus_slots? : Int = 100,
  task_types? : WorkerTaskTypes = WorkerTaskTypes::default(),
  sticky_queue_schedule_to_start_timeout_millis? : UInt64 = 10000UL,
  max_heartbeat_throttle_interval_millis? : UInt64 = 60000UL,
  default_heartbeat_throttle_interval_millis? : UInt64 = 30000UL,
  max_activities_per_second? : Double = 0.0,
  max_task_queue_activities_per_second? : Double = 0.0,
  graceful_shutdown_period_millis? : UInt64 = 0UL,
  workflow_task_poller_behavior? : PollerBehavior = PollerBehavior::simple_maximum(
    5,
  ),
  nonsticky_to_sticky_poll_ratio? : Double = 0.2,
  activity_task_poller_behavior? : PollerBehavior = PollerBehavior::simple_maximum(
    5,
  ),
  nexus_task_poller_behavior? : PollerBehavior = PollerBehavior::simple_maximum(
    5,
  ),
  nondeterminism_as_workflow_fail? : Bool = false,
) -> WorkerOptions {
  {
    namespace_name,
    task_queue,
    build_id,
    identity_override,
    max_cached_workflows,
    workflow_slots,
    activity_slots,
    local_activity_slots,
    nexus_slots,
    task_types,
    sticky_queue_schedule_to_start_timeout_millis,
    max_heartbeat_throttle_interval_millis,
    default_heartbeat_throttle_interval_millis,
    max_activities_per_second,
    max_task_queue_activities_per_second,
    graceful_shutdown_period_millis,
    workflow_task_poller_behavior,
    nonsticky_to_sticky_poll_ratio,
    activity_task_poller_behavior,
    nexus_task_poller_behavior,
    nondeterminism_as_workflow_fail,
  }
}

///|
/// Options for starting Temporal Core's ephemeral test server.
///
/// If `existing_path` is empty, Core may download a server binary according to
/// `download_version`, `download_dest_dir`, and `download_ttl_seconds`.
pub(all) struct TestServerOptions {
  existing_path : String
  sdk_name : String
  sdk_version : String
  download_version : String
  download_dest_dir : String
  port : Int
  extra_args : String
  download_ttl_seconds : UInt64
}

///|
/// Returns default test server options.
pub fn TestServerOptions::default() -> TestServerOptions {
  {
    existing_path: "",
    sdk_name: "temporal.mbt",
    sdk_version: "0.0.1",
    download_version: "default",
    download_dest_dir: "",
    port: 0,
    extra_args: "",
    download_ttl_seconds: 0UL,
  }
}

///|
/// Options for starting Temporal Core's ephemeral development server.
///
/// The embedded `test_server` options control binary discovery or download.
pub(all) struct DevServerOptions {
  test_server : TestServerOptions
  namespace_name : String
  ip : String
  database_filename : String
  ui : Bool
  ui_port : Int
  log_format : String
  log_level : String
}

///|
/// Returns default development server options for a local `default` namespace.
pub fn DevServerOptions::default() -> DevServerOptions {
  {
    test_server: TestServerOptions::default(),
    namespace_name: "default",
    ip: "127.0.0.1",
    database_filename: "",
    ui: false,
    ui_port: 0,
    log_format: "pretty",
    log_level: "warn",
  }
}
