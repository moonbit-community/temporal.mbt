# temporal.mbt

`temporal.mbt` is an experimental, low-level MoonBit native binding for Temporal Core. It calls the Temporal Rust SDK `sdk-core-c-bridge` through a small C stub and exposes Core handles plus protobuf payloads as MoonBit values. Application code is expected to encode request protobufs and decode response protobufs outside this package.

## Status

- Target: MoonBit `native` builds on Linux.
- API level: low-level Core handles and raw protobuf `Bytes`.
- Stability: experimental. Names and option shapes may change while the binding
  tracks the Temporal SDK Core bridge.
- Build model: Cargo builds the vendored Rust bridge as a static library during
  MoonBit native builds.

## What Is Included

- `Runtime` for owning the Temporal Core runtime.
- `Connection` for connecting to Temporal services and making raw RPC calls.
- `Worker` for polling and completing Core worker tasks with protobuf bytes.
- `CancellationToken` for cancelling in-flight RPC calls.
- `Random` for deterministic Core-compatible random values.
- `EphemeralServer` helpers for starting Temporal test/dev servers through Core.
- Structured MoonBit option types for runtime, connection, RPC, worker, and
  ephemeral server configuration.

## Build Requirements

Normal package builds require:

- MoonBit toolchain with native target support.
- Rust/Cargo toolchain.
- A C compiler and linker for MoonBit native stubs.

Normal builds do not require Node.js, a checked-out Temporal SDK submodule, or a
system `protoc`. The committed vendor snapshot includes the files needed by
Cargo, including the generated protobuf descriptor set.

The package pre-build hook in `moon.pkg` runs Cargo when the static library is
missing or older than the vendor stamp:

```sh
cargo build --manifest-path vendor/temporal-sdk-core/Cargo.toml \
  -p temporalio-sdk-core-c-bridge --release
```

The generated Cargo output is local build state and is ignored by git:

```text
vendor/temporal-sdk-core/target/
```

## Quick Check

Use the native target. The first run may take several minutes because it builds
the vendored Rust bridge.

```sh
moon check --target native
moon test --target native
```

The vendor snapshot can be checked against the pinned `temporal-sdk` submodule
commit:

```sh
node scripts/vendor_temporal_core.mjs --check
```

Refreshing the vendor snapshot is a maintainer operation:

```sh
node scripts/vendor_temporal_core.mjs
```

That command copies the required Temporal SDK Core files into
`vendor/temporal-sdk-core`, writes `Cargo.lock`, writes
`crates/common/generated/descriptors.bin`, and updates `VENDOR.json` plus
`VENDOR.stamp`.

## Ownership And Lifetimes

The handle types are opaque external objects:

- `Runtime`
- `Connection`
- `Worker`
- `CancellationToken`
- `Random`
- `EphemeralServer`

They wrap Rust Core bridge pointers managed by the C shim. MoonBit finalizers
release the underlying Core objects when the MoonBit handles become unreachable.
Code should still shut down long-running Core resources explicitly when the API
offers a shutdown method, for example `Worker::initiate_shutdown`,
`Worker::finalize_shutdown_blocking`, or `EphemeralServer::shutdown_blocking`.

## Protobuf Bytes

RPC and worker APIs use `Bytes` for protobuf payloads:

- `RpcCallOptions::request` is an encoded Temporal request message.
- `Connection::rpc_call_blocking` returns the encoded response message.
- Worker poll methods return encoded activation/task messages.
- Worker completion and heartbeat methods accept encoded completion/heartbeat
  messages.

This package does not validate the protobuf schema at the MoonBit layer. If the
bytes do not match the RPC or worker method you call, Temporal Core will return
an error or the server will reject the request.

## Metadata Encoding

`ConnectionOptions`, `RpcCallOptions`, and update methods expose `metadata` and
`binary_metadata` as `Bytes` to keep the public API small. The C shim interprets
the bytes as newline-separated key/value pairs:

```text
key1
value1
key2
value2
```

Keys and values should not contain newline bytes. Use `metadata` for textual
metadata and `binary_metadata` for binary gRPC metadata.

## Creating A Runtime

```text
let runtime = match Runtime::new() {
  Ok(runtime) => runtime
  Err(err) => abort(err.message)
}
```

`RuntimeOptions::default()` lets Core choose the default worker heartbeat
interval. Set `worker_heartbeat_interval_millis` when tests or embedding code
need tighter control.

## Connecting To Temporal

```text
let options = ConnectionOptions::new(
  "http://127.0.0.1:7233",
  client_name="temporal.mbt-example",
  client_version="0.1.0",
  identity="example-worker",
)

let connection = match Connection::connect_blocking(runtime, options) {
  Ok(connection) => connection
  Err(err) => abort(err.message)
}
```

`Connection::connect` currently calls the blocking implementation and invokes
the callback before returning. It is provided as a callback-shaped convenience,
not as a scheduler-backed async primitive.

API keys and metadata can be updated after connecting:

```text
connection.update_api_key("new-api-key")
connection.update_metadata(b"authorization\nBearer token")
```

## Raw RPC Calls

Use `RpcCallOptions` when you have encoded protobuf request bytes and know the
Temporal service plus RPC name.

```text
let call = RpcCallOptions::new(
  Workflow,
  "GetSystemInfo",
  request=b"",
  timeout_millis=5000U,
)

match connection.rpc_call_blocking(call) {
  Ok(response_bytes) => {
    // Decode temporal.api.workflowservice.v1.GetSystemInfoResponse here.
    ignore(response_bytes)
  }
  Err(failure) => {
    abort(failure.message)
  }
}
```

`RpcFailure::status_code` is the gRPC status code from Core. `details` contains
encoded failure details when Core provides them.

Use `CancellationToken` for calls that may need external cancellation:

```text
let token = CancellationToken::new()
let call = RpcCallOptions::new(
  Workflow,
  "GetSystemInfo",
  cancellation_token=token,
)
token.cancel()
ignore(connection.rpc_call_blocking(call))
```

## Workers

Workers are created from an existing `Connection` and `WorkerOptions`:

```text
let worker_options = WorkerOptions::new(
  "default",
  "moonbit-task-queue",
  build_id="temporal.mbt-example",
)

let worker = match Worker::new(connection, worker_options) {
  Ok(worker) => worker
  Err(err) => abort(err.message)
}
```

Poll methods return `Ok(None)` after worker shutdown and `Ok(Some(bytes))` when
a task was received:

```text
match worker.poll_workflow_activation_blocking() {
  Ok(Some(activation)) => {
    // Decode the activation protobuf and eventually complete it.
    ignore(activation)
  }
  Ok(None) => ()
  Err(err) => abort(err.message)
}
```

Completion methods accept the corresponding encoded Core completion protobuf:

```text
let completion : Bytes = b"...encoded completion..."
match worker.complete_workflow_activation_blocking(completion) {
  Ok(()) => ()
  Err(err) => abort(err.message)
}
```

Shutdown is explicit:

```text
worker.initiate_shutdown()
ignore(worker.finalize_shutdown_blocking())
```

## Ephemeral Servers

Core can start Temporal test and dev servers for integration tests. These APIs
may download Temporal server binaries unless `existing_path` is provided.

```text
let options = DevServerOptions::default()
let (server, target) = match EphemeralServer::start_dev_server_blocking(runtime, options) {
  Ok(pair) => pair
  Err(err) => abort(err.message)
}

let connection = Connection::connect_blocking(
  runtime,
  ConnectionOptions::new(target),
)

ignore(connection)
ignore(server.shutdown_blocking())
```

## Deterministic Random

`Random` wraps Core's deterministic random source:

```text
test "deterministic random" {
  let random = Random::new(123UL)
  let value = random.int32_range(1, 10)
  assert_true(value >= 1 && value < 10)

  let bytes = random.fill_bytes(8)
  assert_eq(bytes.length(), 8)
}
```

`max_inclusive=false` means the upper bound is exclusive. Pass
`max_inclusive=true` when the upper bound should be included.

## Error Handling

Setup and worker lifecycle APIs return `Result[..., TemporalError]`.
`TemporalError::message` is a UTF-8 lossy string converted from Core's error
message bytes.

RPC APIs return `Result[Bytes, RpcFailure]` because Core provides structured
gRPC failure data for RPC calls.

## Maintaining The Vendor Snapshot

`temporal-sdk` is kept as the refresh source. `vendor/temporal-sdk-core` is the
build input used by normal package users.

After updating the submodule, refresh and verify:

```sh
node scripts/vendor_temporal_core.mjs
node scripts/vendor_temporal_core.mjs --check
moon test --target native
```

The refresh script writes `VENDOR.stamp`. `moon.pkg` uses that stamp as the
pre-build input and the Rust static library as the pre-build output, so Cargo is
rerun when the vendor snapshot changes while `vendor/temporal-sdk-core/target`
remains untracked.
