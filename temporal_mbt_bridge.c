#include <moonbit.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct TemporalCoreCustomMetricMeter;
struct TemporalCoreCustomSlotSupplierCallbacks;

#include "temporal-sdk-core-c-bridge.h"

typedef struct {
  TemporalCoreRuntime *ptr;
} TemporalMbtRuntime;

typedef struct {
  TemporalCoreConnection *ptr;
  TemporalMbtRuntime *runtime;
} TemporalMbtConnection;

typedef struct {
  TemporalCoreWorker *ptr;
  TemporalMbtRuntime *runtime;
} TemporalMbtWorker;

typedef struct {
  TemporalCoreCancellationToken *ptr;
} TemporalMbtCancellationToken;

typedef struct {
  TemporalCoreRandom *ptr;
} TemporalMbtRandom;

typedef struct {
  TemporalCoreEphemeralServer *ptr;
  TemporalMbtRuntime *runtime;
} TemporalMbtEphemeralServer;

typedef enum {
  TEMPORAL_MBT_RESULT_NONE = 0,
  TEMPORAL_MBT_RESULT_RUNTIME = 1,
  TEMPORAL_MBT_RESULT_CONNECTION = 2,
  TEMPORAL_MBT_RESULT_WORKER = 3,
  TEMPORAL_MBT_RESULT_EPHEMERAL_SERVER = 4,
} TemporalMbtResultKind;

typedef struct {
  bool ok;
  bool shutdown;
  uint32_t status_code;
  TemporalMbtResultKind kind;
  void *handle;
  TemporalMbtRuntime *runtime;
  uint8_t *success;
  size_t success_len;
  uint8_t *message;
  size_t message_len;
  uint8_t *details;
  size_t details_len;
  uint8_t *target;
  size_t target_len;
} TemporalMbtCoreResult;

typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  bool done;
} TemporalMbtWait;

typedef struct {
  TemporalCoreByteArrayRef *data;
  size_t size;
} TemporalMbtMetadata;

static TemporalCoreByteArrayRef temporal_mbt_bytes_ref(moonbit_bytes_t bytes) {
  TemporalCoreByteArrayRef ref = { .data = bytes, .size = Moonbit_array_length(bytes) };
  return ref;
}

static uint8_t *temporal_mbt_copy(const uint8_t *data, size_t len) {
  if (len == 0) {
    return NULL;
  }
  uint8_t *copy = (uint8_t *)malloc(len);
  if (copy == NULL) {
    abort();
  }
  memcpy(copy, data, len);
  return copy;
}

static void temporal_mbt_set_copy(uint8_t **dst, size_t *dst_len, const uint8_t *data, size_t len) {
  free(*dst);
  *dst = temporal_mbt_copy(data, len);
  *dst_len = len;
}

static void temporal_mbt_copy_core_bytes(
  uint8_t **dst,
  size_t *dst_len,
  const TemporalCoreByteArray *bytes
) {
  if (bytes == NULL) {
    temporal_mbt_set_copy(dst, dst_len, NULL, 0);
  } else {
    temporal_mbt_set_copy(dst, dst_len, bytes->data, bytes->size);
  }
}

static moonbit_bytes_t temporal_mbt_make_bytes(const uint8_t *data, size_t len) {
  if (len > INT32_MAX) {
    abort();
  }
  moonbit_bytes_t bytes = moonbit_make_bytes((int32_t)len, 0);
  if (len != 0) {
    memcpy(bytes, data, len);
  }
  return bytes;
}

static void temporal_mbt_wait_init(TemporalMbtWait *wait) {
  pthread_mutex_init(&wait->mutex, NULL);
  pthread_cond_init(&wait->cond, NULL);
  wait->done = false;
}

static void temporal_mbt_wait_signal(TemporalMbtWait *wait) {
  pthread_mutex_lock(&wait->mutex);
  wait->done = true;
  pthread_cond_signal(&wait->cond);
  pthread_mutex_unlock(&wait->mutex);
}

static void temporal_mbt_wait_finish(TemporalMbtWait *wait) {
  pthread_mutex_lock(&wait->mutex);
  while (!wait->done) {
    pthread_cond_wait(&wait->cond, &wait->mutex);
  }
  pthread_mutex_unlock(&wait->mutex);
  pthread_cond_destroy(&wait->cond);
  pthread_mutex_destroy(&wait->mutex);
}

static void temporal_mbt_runtime_finalize(void *ptr) {
  TemporalMbtRuntime *runtime = (TemporalMbtRuntime *)ptr;
  if (runtime->ptr != NULL) {
    temporal_core_runtime_free(runtime->ptr);
    runtime->ptr = NULL;
  }
}

static void temporal_mbt_connection_finalize(void *ptr) {
  TemporalMbtConnection *connection = (TemporalMbtConnection *)ptr;
  if (connection->ptr != NULL) {
    temporal_core_client_free(connection->ptr);
    connection->ptr = NULL;
  }
  if (connection->runtime != NULL) {
    moonbit_decref(connection->runtime);
    connection->runtime = NULL;
  }
}

static void temporal_mbt_worker_finalize(void *ptr) {
  TemporalMbtWorker *worker = (TemporalMbtWorker *)ptr;
  if (worker->ptr != NULL) {
    temporal_core_worker_free(worker->ptr);
    worker->ptr = NULL;
  }
  if (worker->runtime != NULL) {
    moonbit_decref(worker->runtime);
    worker->runtime = NULL;
  }
}

static void temporal_mbt_cancellation_token_finalize(void *ptr) {
  TemporalMbtCancellationToken *token = (TemporalMbtCancellationToken *)ptr;
  if (token->ptr != NULL) {
    temporal_core_cancellation_token_free(token->ptr);
    token->ptr = NULL;
  }
}

static void temporal_mbt_random_finalize(void *ptr) {
  TemporalMbtRandom *random = (TemporalMbtRandom *)ptr;
  if (random->ptr != NULL) {
    temporal_core_random_free(random->ptr);
    random->ptr = NULL;
  }
}

static void temporal_mbt_ephemeral_server_finalize(void *ptr) {
  TemporalMbtEphemeralServer *server = (TemporalMbtEphemeralServer *)ptr;
  if (server->ptr != NULL) {
    temporal_core_ephemeral_server_free(server->ptr);
    server->ptr = NULL;
  }
  if (server->runtime != NULL) {
    moonbit_decref(server->runtime);
    server->runtime = NULL;
  }
}

static void temporal_mbt_result_free_handle(TemporalMbtCoreResult *result) {
  if (result->handle == NULL) {
    return;
  }
  switch (result->kind) {
    case TEMPORAL_MBT_RESULT_RUNTIME:
      temporal_core_runtime_free((TemporalCoreRuntime *)result->handle);
      break;
    case TEMPORAL_MBT_RESULT_CONNECTION:
      temporal_core_client_free((TemporalCoreConnection *)result->handle);
      break;
    case TEMPORAL_MBT_RESULT_WORKER:
      temporal_core_worker_free((TemporalCoreWorker *)result->handle);
      break;
    case TEMPORAL_MBT_RESULT_EPHEMERAL_SERVER:
      temporal_core_ephemeral_server_free((TemporalCoreEphemeralServer *)result->handle);
      break;
    case TEMPORAL_MBT_RESULT_NONE:
      break;
  }
  result->handle = NULL;
}

static void temporal_mbt_core_result_finalize(void *ptr) {
  TemporalMbtCoreResult *result = (TemporalMbtCoreResult *)ptr;
  temporal_mbt_result_free_handle(result);
  if (result->runtime != NULL) {
    moonbit_decref(result->runtime);
    result->runtime = NULL;
  }
  free(result->success);
  free(result->message);
  free(result->details);
  free(result->target);
}

static TemporalMbtCoreResult *temporal_mbt_result_new(void) {
  TemporalMbtCoreResult *result = (TemporalMbtCoreResult *)moonbit_make_external_object(
    temporal_mbt_core_result_finalize,
    sizeof(TemporalMbtCoreResult)
  );
  memset(result, 0, sizeof(*result));
  return result;
}

static TemporalMbtRuntime *temporal_mbt_runtime_wrap(TemporalCoreRuntime *ptr) {
  TemporalMbtRuntime *runtime = (TemporalMbtRuntime *)moonbit_make_external_object(
    temporal_mbt_runtime_finalize,
    sizeof(TemporalMbtRuntime)
  );
  runtime->ptr = ptr;
  return runtime;
}

static TemporalMbtConnection *temporal_mbt_connection_wrap(
  TemporalCoreConnection *ptr,
  TemporalMbtRuntime *runtime
) {
  TemporalMbtConnection *connection = (TemporalMbtConnection *)moonbit_make_external_object(
    temporal_mbt_connection_finalize,
    sizeof(TemporalMbtConnection)
  );
  connection->ptr = ptr;
  connection->runtime = runtime;
  return connection;
}

static TemporalMbtWorker *temporal_mbt_worker_wrap(
  TemporalCoreWorker *ptr,
  TemporalMbtRuntime *runtime
) {
  TemporalMbtWorker *worker = (TemporalMbtWorker *)moonbit_make_external_object(
    temporal_mbt_worker_finalize,
    sizeof(TemporalMbtWorker)
  );
  worker->ptr = ptr;
  worker->runtime = runtime;
  return worker;
}

static TemporalMbtEphemeralServer *temporal_mbt_ephemeral_server_wrap(
  TemporalCoreEphemeralServer *ptr,
  TemporalMbtRuntime *runtime
) {
  TemporalMbtEphemeralServer *server = (TemporalMbtEphemeralServer *)moonbit_make_external_object(
    temporal_mbt_ephemeral_server_finalize,
    sizeof(TemporalMbtEphemeralServer)
  );
  server->ptr = ptr;
  server->runtime = runtime;
  return server;
}

static TemporalMbtMetadata temporal_mbt_metadata_from_bytes(moonbit_bytes_t bytes) {
  const uint8_t *data = bytes;
  size_t len = Moonbit_array_length(bytes);
  size_t count = 0;
  size_t pos = 0;
  while (pos < len) {
    const uint8_t *key_start = data + pos;
    const uint8_t *mid = memchr(key_start, '\n', len - pos);
    if (mid == NULL) {
      break;
    }
    size_t value_pos = (size_t)(mid - data) + 1;
    const uint8_t *end = memchr(data + value_pos, '\n', len - value_pos);
    pos = end == NULL ? len : (size_t)(end - data) + 1;
    count += 1;
  }
  TemporalCoreByteArrayRef *items = NULL;
  if (count != 0) {
    items = (TemporalCoreByteArrayRef *)calloc(count, sizeof(TemporalCoreByteArrayRef));
    if (items == NULL) {
      abort();
    }
  }
  pos = 0;
  size_t idx = 0;
  while (idx < count && pos < len) {
    const uint8_t *key_start = data + pos;
    const uint8_t *mid = memchr(key_start, '\n', len - pos);
    size_t value_pos = (size_t)(mid - data) + 1;
    const uint8_t *end = memchr(data + value_pos, '\n', len - value_pos);
    size_t end_pos = end == NULL ? len : (size_t)(end - data);
    items[idx].data = key_start;
    items[idx].size = end_pos - pos;
    pos = end == NULL ? len : end_pos + 1;
    idx += 1;
  }
  TemporalMbtMetadata metadata = { .data = items, .size = count };
  return metadata;
}

static void temporal_mbt_metadata_free(TemporalMbtMetadata *metadata) {
  free(metadata->data);
  metadata->data = NULL;
  metadata->size = 0;
}

static TemporalCoreByteArrayRefArray temporal_mbt_metadata_ref(TemporalMbtMetadata *metadata) {
  TemporalCoreByteArrayRefArray ref = { .data = metadata->data, .size = metadata->size };
  return ref;
}

MOONBIT_FFI_EXPORT
bool temporal_mbt_core_result_is_ok(TemporalMbtCoreResult *result) {
  return result->ok;
}

MOONBIT_FFI_EXPORT
bool temporal_mbt_core_result_is_shutdown(TemporalMbtCoreResult *result) {
  return result->shutdown;
}

MOONBIT_FFI_EXPORT
int32_t temporal_mbt_core_result_status_code(TemporalMbtCoreResult *result) {
  return (int32_t)result->status_code;
}

MOONBIT_FFI_EXPORT
moonbit_bytes_t temporal_mbt_core_result_error_message(TemporalMbtCoreResult *result) {
  return temporal_mbt_make_bytes(result->message, result->message_len);
}

MOONBIT_FFI_EXPORT
moonbit_bytes_t temporal_mbt_core_result_failure_details(TemporalMbtCoreResult *result) {
  return temporal_mbt_make_bytes(result->details, result->details_len);
}

MOONBIT_FFI_EXPORT
moonbit_bytes_t temporal_mbt_core_result_success_bytes(TemporalMbtCoreResult *result) {
  return temporal_mbt_make_bytes(result->success, result->success_len);
}

MOONBIT_FFI_EXPORT
moonbit_bytes_t temporal_mbt_core_result_target(TemporalMbtCoreResult *result) {
  return temporal_mbt_make_bytes(result->target, result->target_len);
}

MOONBIT_FFI_EXPORT
TemporalMbtRuntime *temporal_mbt_core_result_take_runtime(TemporalMbtCoreResult *result) {
  if (!result->ok || result->kind != TEMPORAL_MBT_RESULT_RUNTIME || result->handle == NULL) {
    return NULL;
  }
  TemporalCoreRuntime *raw = (TemporalCoreRuntime *)result->handle;
  result->handle = NULL;
  result->kind = TEMPORAL_MBT_RESULT_NONE;
  return temporal_mbt_runtime_wrap(raw);
}

MOONBIT_FFI_EXPORT
TemporalMbtConnection *temporal_mbt_core_result_take_connection(TemporalMbtCoreResult *result) {
  if (!result->ok || result->kind != TEMPORAL_MBT_RESULT_CONNECTION || result->handle == NULL) {
    return NULL;
  }
  TemporalCoreConnection *raw = (TemporalCoreConnection *)result->handle;
  TemporalMbtRuntime *runtime = result->runtime;
  result->handle = NULL;
  result->runtime = NULL;
  result->kind = TEMPORAL_MBT_RESULT_NONE;
  return temporal_mbt_connection_wrap(raw, runtime);
}

MOONBIT_FFI_EXPORT
TemporalMbtWorker *temporal_mbt_core_result_take_worker(TemporalMbtCoreResult *result) {
  if (!result->ok || result->kind != TEMPORAL_MBT_RESULT_WORKER || result->handle == NULL) {
    return NULL;
  }
  TemporalCoreWorker *raw = (TemporalCoreWorker *)result->handle;
  TemporalMbtRuntime *runtime = result->runtime;
  result->handle = NULL;
  result->runtime = NULL;
  result->kind = TEMPORAL_MBT_RESULT_NONE;
  return temporal_mbt_worker_wrap(raw, runtime);
}

MOONBIT_FFI_EXPORT
TemporalMbtEphemeralServer *temporal_mbt_core_result_take_ephemeral_server(
  TemporalMbtCoreResult *result
) {
  if (
    !result->ok || result->kind != TEMPORAL_MBT_RESULT_EPHEMERAL_SERVER ||
    result->handle == NULL
  ) {
    return NULL;
  }
  TemporalCoreEphemeralServer *raw = (TemporalCoreEphemeralServer *)result->handle;
  TemporalMbtRuntime *runtime = result->runtime;
  result->handle = NULL;
  result->runtime = NULL;
  result->kind = TEMPORAL_MBT_RESULT_NONE;
  return temporal_mbt_ephemeral_server_wrap(raw, runtime);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_runtime_new(uint64_t worker_heartbeat_interval_millis) {
  TemporalCoreRuntimeOptions options;
  memset(&options, 0, sizeof(options));
  options.worker_heartbeat_interval_millis = worker_heartbeat_interval_millis;
  TemporalCoreRuntimeOrFail raw = temporal_core_runtime_new(&options);
  TemporalMbtCoreResult *result = temporal_mbt_result_new();
  if (raw.fail == NULL) {
    result->ok = true;
    result->kind = TEMPORAL_MBT_RESULT_RUNTIME;
    result->handle = raw.runtime;
  } else {
    result->ok = false;
    temporal_mbt_copy_core_bytes(&result->message, &result->message_len, raw.fail);
    temporal_core_byte_array_free(raw.runtime, raw.fail);
    temporal_core_runtime_free(raw.runtime);
  }
  return result;
}

typedef struct {
  TemporalMbtWait wait;
  TemporalMbtRuntime *runtime;
  TemporalCoreConnection *connection;
  uint8_t *message;
  size_t message_len;
} TemporalMbtConnectWait;

static void temporal_mbt_connect_callback(
  void *user_data,
  TemporalCoreConnection *success,
  const TemporalCoreByteArray *fail
) {
  TemporalMbtConnectWait *wait = (TemporalMbtConnectWait *)user_data;
  wait->connection = success;
  if (fail != NULL) {
    temporal_mbt_copy_core_bytes(&wait->message, &wait->message_len, fail);
    temporal_core_byte_array_free(wait->runtime->ptr, fail);
  }
  temporal_mbt_wait_signal(&wait->wait);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_client_connect(
  TemporalMbtRuntime *runtime,
  moonbit_bytes_t target_url,
  moonbit_bytes_t client_name,
  moonbit_bytes_t client_version,
  moonbit_bytes_t identity,
  moonbit_bytes_t api_key,
  moonbit_bytes_t metadata_bytes,
  moonbit_bytes_t binary_metadata_bytes
) {
  TemporalMbtMetadata metadata = temporal_mbt_metadata_from_bytes(metadata_bytes);
  TemporalMbtMetadata binary_metadata = temporal_mbt_metadata_from_bytes(binary_metadata_bytes);
  TemporalCoreConnectionOptions options;
  memset(&options, 0, sizeof(options));
  options.target_url = temporal_mbt_bytes_ref(target_url);
  options.client_name = temporal_mbt_bytes_ref(client_name);
  options.client_version = temporal_mbt_bytes_ref(client_version);
  options.identity = temporal_mbt_bytes_ref(identity);
  options.api_key = temporal_mbt_bytes_ref(api_key);
  options.metadata = temporal_mbt_metadata_ref(&metadata);
  options.binary_metadata = temporal_mbt_metadata_ref(&binary_metadata);

  TemporalMbtConnectWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = runtime;
  temporal_core_client_connect(runtime->ptr, &options, &wait, temporal_mbt_connect_callback);
  temporal_mbt_wait_finish(&wait.wait);
  temporal_mbt_metadata_free(&metadata);
  temporal_mbt_metadata_free(&binary_metadata);

  TemporalMbtCoreResult *result = temporal_mbt_result_new();
  if (wait.connection != NULL) {
    result->ok = true;
    result->kind = TEMPORAL_MBT_RESULT_CONNECTION;
    result->handle = wait.connection;
    result->runtime = runtime;
    moonbit_incref(runtime);
  } else {
    result->ok = false;
    result->message = wait.message;
    result->message_len = wait.message_len;
  }
  return result;
}

MOONBIT_FFI_EXPORT
void temporal_mbt_client_update_api_key(TemporalMbtConnection *connection, moonbit_bytes_t api_key) {
  temporal_core_client_update_api_key(connection->ptr, temporal_mbt_bytes_ref(api_key));
}

MOONBIT_FFI_EXPORT
void temporal_mbt_client_update_metadata(TemporalMbtConnection *connection, moonbit_bytes_t bytes) {
  TemporalMbtMetadata metadata = temporal_mbt_metadata_from_bytes(bytes);
  temporal_core_client_update_metadata(connection->ptr, temporal_mbt_metadata_ref(&metadata));
  temporal_mbt_metadata_free(&metadata);
}

MOONBIT_FFI_EXPORT
void temporal_mbt_client_update_binary_metadata(
  TemporalMbtConnection *connection,
  moonbit_bytes_t bytes
) {
  TemporalMbtMetadata metadata = temporal_mbt_metadata_from_bytes(bytes);
  temporal_core_client_update_binary_metadata(connection->ptr, temporal_mbt_metadata_ref(&metadata));
  temporal_mbt_metadata_free(&metadata);
}

typedef struct {
  TemporalMbtWait wait;
  TemporalMbtRuntime *runtime;
  uint8_t *success;
  size_t success_len;
  uint32_t status_code;
  uint8_t *message;
  size_t message_len;
  uint8_t *details;
  size_t details_len;
} TemporalMbtRpcWait;

static void temporal_mbt_rpc_callback(
  void *user_data,
  const TemporalCoreByteArray *success,
  uint32_t status_code,
  const TemporalCoreByteArray *failure_message,
  const TemporalCoreByteArray *failure_details
) {
  TemporalMbtRpcWait *wait = (TemporalMbtRpcWait *)user_data;
  wait->status_code = status_code;
  if (success != NULL) {
    temporal_mbt_copy_core_bytes(&wait->success, &wait->success_len, success);
    temporal_core_byte_array_free(wait->runtime->ptr, success);
  }
  if (failure_message != NULL) {
    temporal_mbt_copy_core_bytes(&wait->message, &wait->message_len, failure_message);
    temporal_core_byte_array_free(wait->runtime->ptr, failure_message);
  }
  if (failure_details != NULL) {
    temporal_mbt_copy_core_bytes(&wait->details, &wait->details_len, failure_details);
    temporal_core_byte_array_free(wait->runtime->ptr, failure_details);
  }
  temporal_mbt_wait_signal(&wait->wait);
}

static TemporalMbtCoreResult *temporal_mbt_client_rpc_call_inner(
  TemporalMbtConnection *connection,
  int32_t service,
  moonbit_bytes_t rpc,
  moonbit_bytes_t request,
  int32_t retry,
  moonbit_bytes_t metadata_bytes,
  moonbit_bytes_t binary_metadata_bytes,
  uint32_t timeout_millis,
  TemporalMbtCancellationToken *cancellation_token
) {
  TemporalMbtMetadata metadata = temporal_mbt_metadata_from_bytes(metadata_bytes);
  TemporalMbtMetadata binary_metadata = temporal_mbt_metadata_from_bytes(binary_metadata_bytes);
  TemporalCoreRpcCallOptions options;
  memset(&options, 0, sizeof(options));
  options.service = (TemporalCoreRpcService)service;
  options.rpc = temporal_mbt_bytes_ref(rpc);
  options.req = temporal_mbt_bytes_ref(request);
  options.retry = retry != 0;
  options.metadata = temporal_mbt_metadata_ref(&metadata);
  options.binary_metadata = temporal_mbt_metadata_ref(&binary_metadata);
  options.timeout_millis = timeout_millis;
  options.cancellation_token = cancellation_token == NULL ? NULL : cancellation_token->ptr;

  TemporalMbtRpcWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = connection->runtime;
  temporal_core_client_rpc_call(connection->ptr, &options, &wait, temporal_mbt_rpc_callback);
  temporal_mbt_wait_finish(&wait.wait);
  temporal_mbt_metadata_free(&metadata);
  temporal_mbt_metadata_free(&binary_metadata);

  TemporalMbtCoreResult *result = temporal_mbt_result_new();
  result->status_code = wait.status_code;
  if (wait.success != NULL) {
    result->ok = true;
    result->success = wait.success;
    result->success_len = wait.success_len;
  } else {
    result->ok = false;
    result->message = wait.message;
    result->message_len = wait.message_len;
    result->details = wait.details;
    result->details_len = wait.details_len;
  }
  return result;
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_client_rpc_call(
  TemporalMbtConnection *connection,
  int32_t service,
  moonbit_bytes_t rpc,
  moonbit_bytes_t request,
  int32_t retry,
  moonbit_bytes_t metadata,
  moonbit_bytes_t binary_metadata,
  uint32_t timeout_millis
) {
  return temporal_mbt_client_rpc_call_inner(
    connection,
    service,
    rpc,
    request,
    retry,
    metadata,
    binary_metadata,
    timeout_millis,
    NULL
  );
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_client_rpc_call_with_cancel(
  TemporalMbtConnection *connection,
  int32_t service,
  moonbit_bytes_t rpc,
  moonbit_bytes_t request,
  int32_t retry,
  moonbit_bytes_t metadata,
  moonbit_bytes_t binary_metadata,
  uint32_t timeout_millis,
  TemporalMbtCancellationToken *cancellation_token
) {
  return temporal_mbt_client_rpc_call_inner(
    connection,
    service,
    rpc,
    request,
    retry,
    metadata,
    binary_metadata,
    timeout_millis,
    cancellation_token
  );
}

MOONBIT_FFI_EXPORT
TemporalMbtCancellationToken *temporal_mbt_cancellation_token_new(void) {
  TemporalMbtCancellationToken *token = (TemporalMbtCancellationToken *)moonbit_make_external_object(
    temporal_mbt_cancellation_token_finalize,
    sizeof(TemporalMbtCancellationToken)
  );
  token->ptr = temporal_core_cancellation_token_new();
  return token;
}

MOONBIT_FFI_EXPORT
void temporal_mbt_cancellation_token_cancel(TemporalMbtCancellationToken *token) {
  temporal_core_cancellation_token_cancel(token->ptr);
}

MOONBIT_FFI_EXPORT
TemporalMbtRandom *temporal_mbt_random_new(uint64_t seed) {
  TemporalMbtRandom *random = (TemporalMbtRandom *)moonbit_make_external_object(
    temporal_mbt_random_finalize,
    sizeof(TemporalMbtRandom)
  );
  random->ptr = temporal_core_random_new(seed);
  return random;
}

MOONBIT_FFI_EXPORT
int32_t temporal_mbt_random_int32_range(
  TemporalMbtRandom *random,
  int32_t min,
  int32_t max,
  int32_t max_inclusive
) {
  return temporal_core_random_int32_range(random->ptr, min, max, max_inclusive != 0);
}

MOONBIT_FFI_EXPORT
double temporal_mbt_random_double_range(
  TemporalMbtRandom *random,
  double min,
  double max,
  int32_t max_inclusive
) {
  return temporal_core_random_double_range(random->ptr, min, max, max_inclusive != 0);
}

MOONBIT_FFI_EXPORT
moonbit_bytes_t temporal_mbt_random_fill_bytes(TemporalMbtRandom *random, int32_t len) {
  moonbit_bytes_t bytes = moonbit_make_bytes(len, 0);
  TemporalCoreByteArrayRef ref = { .data = bytes, .size = (size_t)len };
  temporal_core_random_fill_bytes(random->ptr, ref);
  return bytes;
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_new(
  TemporalMbtConnection *connection,
  moonbit_bytes_t namespace_,
  moonbit_bytes_t task_queue,
  moonbit_bytes_t build_id,
  moonbit_bytes_t identity_override,
  uint32_t max_cached_workflows,
  int32_t workflow_slots,
  int32_t activity_slots,
  int32_t local_activity_slots,
  int32_t nexus_slots,
  int32_t enable_workflows,
  int32_t enable_local_activities,
  int32_t enable_remote_activities,
  int32_t enable_nexus,
  uint64_t sticky_queue_schedule_to_start_timeout_millis,
  uint64_t max_heartbeat_throttle_interval_millis,
  uint64_t default_heartbeat_throttle_interval_millis,
  double max_activities_per_second,
  double max_task_queue_activities_per_second,
  uint64_t graceful_shutdown_period_millis,
  int32_t workflow_task_poller_maximum,
  double nonsticky_to_sticky_poll_ratio,
  int32_t activity_task_poller_maximum,
  int32_t nexus_task_poller_maximum,
  int32_t nondeterminism_as_workflow_fail
) {
  TemporalCorePollerBehaviorSimpleMaximum wf_poller = {
    .simple_maximum = (uintptr_t)workflow_task_poller_maximum
  };
  TemporalCorePollerBehaviorSimpleMaximum act_poller = {
    .simple_maximum = (uintptr_t)activity_task_poller_maximum
  };
  TemporalCorePollerBehaviorSimpleMaximum nexus_poller = {
    .simple_maximum = (uintptr_t)nexus_task_poller_maximum
  };
  TemporalCoreWorkerOptions options;
  memset(&options, 0, sizeof(options));
  options.namespace_ = temporal_mbt_bytes_ref(namespace_);
  options.task_queue = temporal_mbt_bytes_ref(task_queue);
  options.versioning_strategy.tag = None;
  options.versioning_strategy.none.build_id = temporal_mbt_bytes_ref(build_id);
  options.identity_override = temporal_mbt_bytes_ref(identity_override);
  options.max_cached_workflows = max_cached_workflows;
  options.tuner.workflow_slot_supplier.tag = FixedSize;
  options.tuner.workflow_slot_supplier.fixed_size.num_slots = (uintptr_t)workflow_slots;
  options.tuner.activity_slot_supplier.tag = FixedSize;
  options.tuner.activity_slot_supplier.fixed_size.num_slots = (uintptr_t)activity_slots;
  options.tuner.local_activity_slot_supplier.tag = FixedSize;
  options.tuner.local_activity_slot_supplier.fixed_size.num_slots = (uintptr_t)local_activity_slots;
  options.tuner.nexus_task_slot_supplier.tag = FixedSize;
  options.tuner.nexus_task_slot_supplier.fixed_size.num_slots = (uintptr_t)nexus_slots;
  options.task_types.enable_workflows = enable_workflows != 0;
  options.task_types.enable_local_activities = enable_local_activities != 0;
  options.task_types.enable_remote_activities = enable_remote_activities != 0;
  options.task_types.enable_nexus = enable_nexus != 0;
  options.sticky_queue_schedule_to_start_timeout_millis =
    sticky_queue_schedule_to_start_timeout_millis;
  options.max_heartbeat_throttle_interval_millis = max_heartbeat_throttle_interval_millis;
  options.default_heartbeat_throttle_interval_millis = default_heartbeat_throttle_interval_millis;
  options.max_activities_per_second = max_activities_per_second;
  options.max_task_queue_activities_per_second = max_task_queue_activities_per_second;
  options.graceful_shutdown_period_millis = graceful_shutdown_period_millis;
  options.workflow_task_poller_behavior.simple_maximum = &wf_poller;
  options.activity_task_poller_behavior.simple_maximum = &act_poller;
  options.nexus_task_poller_behavior.simple_maximum = &nexus_poller;
  options.nonsticky_to_sticky_poll_ratio = (float)nonsticky_to_sticky_poll_ratio;
  options.nondeterminism_as_workflow_fail = nondeterminism_as_workflow_fail != 0;

  TemporalCoreWorkerOrFail raw = temporal_core_worker_new(connection->ptr, &options);
  TemporalMbtCoreResult *result = temporal_mbt_result_new();
  if (raw.fail == NULL) {
    result->ok = true;
    result->kind = TEMPORAL_MBT_RESULT_WORKER;
    result->handle = raw.worker;
    result->runtime = connection->runtime;
    moonbit_incref(connection->runtime);
  } else {
    result->ok = false;
    temporal_mbt_copy_core_bytes(&result->message, &result->message_len, raw.fail);
    temporal_core_byte_array_free(connection->runtime->ptr, raw.fail);
  }
  return result;
}

typedef struct {
  TemporalMbtWait wait;
  TemporalMbtRuntime *runtime;
  uint8_t *message;
  size_t message_len;
} TemporalMbtUnitWait;

static void temporal_mbt_unit_callback(void *user_data, const TemporalCoreByteArray *fail) {
  TemporalMbtUnitWait *wait = (TemporalMbtUnitWait *)user_data;
  if (fail != NULL) {
    temporal_mbt_copy_core_bytes(&wait->message, &wait->message_len, fail);
    temporal_core_byte_array_free(wait->runtime->ptr, fail);
  }
  temporal_mbt_wait_signal(&wait->wait);
}

static TemporalMbtCoreResult *temporal_mbt_unit_result_from_wait(TemporalMbtUnitWait *wait) {
  TemporalMbtCoreResult *result = temporal_mbt_result_new();
  if (wait->message == NULL) {
    result->ok = true;
  } else {
    result->ok = false;
    result->message = wait->message;
    result->message_len = wait->message_len;
  }
  return result;
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_validate(TemporalMbtWorker *worker) {
  TemporalMbtUnitWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = worker->runtime;
  temporal_core_worker_validate(worker->ptr, &wait, temporal_mbt_unit_callback);
  temporal_mbt_wait_finish(&wait.wait);
  return temporal_mbt_unit_result_from_wait(&wait);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_replace_client(
  TemporalMbtWorker *worker,
  TemporalMbtConnection *connection
) {
  const TemporalCoreByteArray *fail = temporal_core_worker_replace_client(worker->ptr, connection->ptr);
  TemporalMbtCoreResult *result = temporal_mbt_result_new();
  if (fail == NULL) {
    result->ok = true;
  } else {
    result->ok = false;
    temporal_mbt_copy_core_bytes(&result->message, &result->message_len, fail);
    temporal_core_byte_array_free(worker->runtime->ptr, fail);
  }
  return result;
}

typedef struct {
  TemporalMbtWait wait;
  TemporalMbtRuntime *runtime;
  uint8_t *success;
  size_t success_len;
  uint8_t *message;
  size_t message_len;
  bool shutdown;
} TemporalMbtPollWait;

static void temporal_mbt_poll_callback(
  void *user_data,
  const TemporalCoreByteArray *success,
  const TemporalCoreByteArray *fail
) {
  TemporalMbtPollWait *wait = (TemporalMbtPollWait *)user_data;
  if (success != NULL) {
    temporal_mbt_copy_core_bytes(&wait->success, &wait->success_len, success);
    temporal_core_byte_array_free(wait->runtime->ptr, success);
  } else if (fail != NULL) {
    temporal_mbt_copy_core_bytes(&wait->message, &wait->message_len, fail);
    temporal_core_byte_array_free(wait->runtime->ptr, fail);
  } else {
    wait->shutdown = true;
  }
  temporal_mbt_wait_signal(&wait->wait);
}

static TemporalMbtCoreResult *temporal_mbt_poll_result(TemporalMbtWorker *worker, TemporalMbtPollWait *wait) {
  (void)worker;
  TemporalMbtCoreResult *result = temporal_mbt_result_new();
  if (wait->message != NULL) {
    result->ok = false;
    result->message = wait->message;
    result->message_len = wait->message_len;
  } else {
    result->ok = true;
    result->shutdown = wait->shutdown;
    result->success = wait->success;
    result->success_len = wait->success_len;
  }
  return result;
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_poll_workflow_activation(TemporalMbtWorker *worker) {
  TemporalMbtPollWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = worker->runtime;
  temporal_core_worker_poll_workflow_activation(worker->ptr, &wait, temporal_mbt_poll_callback);
  temporal_mbt_wait_finish(&wait.wait);
  return temporal_mbt_poll_result(worker, &wait);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_poll_activity_task(TemporalMbtWorker *worker) {
  TemporalMbtPollWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = worker->runtime;
  temporal_core_worker_poll_activity_task(worker->ptr, &wait, temporal_mbt_poll_callback);
  temporal_mbt_wait_finish(&wait.wait);
  return temporal_mbt_poll_result(worker, &wait);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_poll_nexus_task(TemporalMbtWorker *worker) {
  TemporalMbtPollWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = worker->runtime;
  temporal_core_worker_poll_nexus_task(worker->ptr, &wait, temporal_mbt_poll_callback);
  temporal_mbt_wait_finish(&wait.wait);
  return temporal_mbt_poll_result(worker, &wait);
}

static TemporalMbtCoreResult *temporal_mbt_worker_complete(
  TemporalMbtWorker *worker,
  moonbit_bytes_t bytes,
  int op
) {
  TemporalMbtUnitWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = worker->runtime;
  if (op == 1) {
    temporal_core_worker_complete_workflow_activation(
      worker->ptr,
      temporal_mbt_bytes_ref(bytes),
      &wait,
      temporal_mbt_unit_callback
    );
  } else if (op == 2) {
    temporal_core_worker_complete_activity_task(
      worker->ptr,
      temporal_mbt_bytes_ref(bytes),
      &wait,
      temporal_mbt_unit_callback
    );
  } else {
    temporal_core_worker_complete_nexus_task(
      worker->ptr,
      temporal_mbt_bytes_ref(bytes),
      &wait,
      temporal_mbt_unit_callback
    );
  }
  temporal_mbt_wait_finish(&wait.wait);
  return temporal_mbt_unit_result_from_wait(&wait);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_complete_workflow_activation(
  TemporalMbtWorker *worker,
  moonbit_bytes_t completion
) {
  return temporal_mbt_worker_complete(worker, completion, 1);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_complete_activity_task(
  TemporalMbtWorker *worker,
  moonbit_bytes_t completion
) {
  return temporal_mbt_worker_complete(worker, completion, 2);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_complete_nexus_task(
  TemporalMbtWorker *worker,
  moonbit_bytes_t completion
) {
  return temporal_mbt_worker_complete(worker, completion, 3);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_record_activity_heartbeat(
  TemporalMbtWorker *worker,
  moonbit_bytes_t heartbeat
) {
  const TemporalCoreByteArray *fail = temporal_core_worker_record_activity_heartbeat(
    worker->ptr,
    temporal_mbt_bytes_ref(heartbeat)
  );
  TemporalMbtCoreResult *result = temporal_mbt_result_new();
  if (fail == NULL) {
    result->ok = true;
  } else {
    result->ok = false;
    temporal_mbt_copy_core_bytes(&result->message, &result->message_len, fail);
    temporal_core_byte_array_free(worker->runtime->ptr, fail);
  }
  return result;
}

MOONBIT_FFI_EXPORT
void temporal_mbt_worker_request_workflow_eviction(TemporalMbtWorker *worker, moonbit_bytes_t run_id) {
  temporal_core_worker_request_workflow_eviction(worker->ptr, temporal_mbt_bytes_ref(run_id));
}

MOONBIT_FFI_EXPORT
void temporal_mbt_worker_initiate_shutdown(TemporalMbtWorker *worker) {
  temporal_core_worker_initiate_shutdown(worker->ptr);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_worker_finalize_shutdown(TemporalMbtWorker *worker) {
  TemporalMbtUnitWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = worker->runtime;
  temporal_core_worker_finalize_shutdown(worker->ptr, &wait, temporal_mbt_unit_callback);
  temporal_mbt_wait_finish(&wait.wait);
  return temporal_mbt_unit_result_from_wait(&wait);
}

typedef struct {
  TemporalMbtWait wait;
  TemporalMbtRuntime *runtime;
  TemporalCoreEphemeralServer *server;
  uint8_t *target;
  size_t target_len;
  uint8_t *message;
  size_t message_len;
} TemporalMbtServerStartWait;

static void temporal_mbt_server_start_callback(
  void *user_data,
  TemporalCoreEphemeralServer *success,
  const TemporalCoreByteArray *success_target,
  const TemporalCoreByteArray *fail
) {
  TemporalMbtServerStartWait *wait = (TemporalMbtServerStartWait *)user_data;
  wait->server = success;
  if (success_target != NULL) {
    temporal_mbt_copy_core_bytes(&wait->target, &wait->target_len, success_target);
    temporal_core_byte_array_free(wait->runtime->ptr, success_target);
  }
  if (fail != NULL) {
    temporal_mbt_copy_core_bytes(&wait->message, &wait->message_len, fail);
    temporal_core_byte_array_free(wait->runtime->ptr, fail);
  }
  temporal_mbt_wait_signal(&wait->wait);
}

static void temporal_mbt_fill_test_server_options(
  TemporalCoreTestServerOptions *options,
  moonbit_bytes_t existing_path,
  moonbit_bytes_t sdk_name,
  moonbit_bytes_t sdk_version,
  moonbit_bytes_t download_version,
  moonbit_bytes_t download_dest_dir,
  int32_t port,
  moonbit_bytes_t extra_args,
  uint64_t download_ttl_seconds
) {
  memset(options, 0, sizeof(*options));
  options->existing_path = temporal_mbt_bytes_ref(existing_path);
  options->sdk_name = temporal_mbt_bytes_ref(sdk_name);
  options->sdk_version = temporal_mbt_bytes_ref(sdk_version);
  options->download_version = temporal_mbt_bytes_ref(download_version);
  options->download_dest_dir = temporal_mbt_bytes_ref(download_dest_dir);
  options->port = (uint16_t)port;
  options->extra_args = temporal_mbt_bytes_ref(extra_args);
  options->download_ttl_seconds = download_ttl_seconds;
}

static TemporalMbtCoreResult *temporal_mbt_server_start_result(
  TemporalMbtRuntime *runtime,
  TemporalMbtServerStartWait *wait
) {
  TemporalMbtCoreResult *result = temporal_mbt_result_new();
  if (wait->server != NULL) {
    result->ok = true;
    result->kind = TEMPORAL_MBT_RESULT_EPHEMERAL_SERVER;
    result->handle = wait->server;
    result->runtime = runtime;
    result->target = wait->target;
    result->target_len = wait->target_len;
    moonbit_incref(runtime);
  } else {
    result->ok = false;
    result->message = wait->message;
    result->message_len = wait->message_len;
  }
  return result;
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_ephemeral_start_test_server(
  TemporalMbtRuntime *runtime,
  moonbit_bytes_t existing_path,
  moonbit_bytes_t sdk_name,
  moonbit_bytes_t sdk_version,
  moonbit_bytes_t download_version,
  moonbit_bytes_t download_dest_dir,
  int32_t port,
  moonbit_bytes_t extra_args,
  uint64_t download_ttl_seconds
) {
  TemporalCoreTestServerOptions options;
  temporal_mbt_fill_test_server_options(
    &options,
    existing_path,
    sdk_name,
    sdk_version,
    download_version,
    download_dest_dir,
    port,
    extra_args,
    download_ttl_seconds
  );
  TemporalMbtServerStartWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = runtime;
  temporal_core_ephemeral_server_start_test_server(
    runtime->ptr,
    &options,
    &wait,
    temporal_mbt_server_start_callback
  );
  temporal_mbt_wait_finish(&wait.wait);
  return temporal_mbt_server_start_result(runtime, &wait);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_ephemeral_start_dev_server(
  TemporalMbtRuntime *runtime,
  moonbit_bytes_t existing_path,
  moonbit_bytes_t sdk_name,
  moonbit_bytes_t sdk_version,
  moonbit_bytes_t download_version,
  moonbit_bytes_t download_dest_dir,
  int32_t port,
  moonbit_bytes_t extra_args,
  uint64_t download_ttl_seconds,
  moonbit_bytes_t namespace_,
  moonbit_bytes_t ip,
  moonbit_bytes_t database_filename,
  int32_t ui,
  int32_t ui_port,
  moonbit_bytes_t log_format,
  moonbit_bytes_t log_level
) {
  TemporalCoreTestServerOptions test_options;
  temporal_mbt_fill_test_server_options(
    &test_options,
    existing_path,
    sdk_name,
    sdk_version,
    download_version,
    download_dest_dir,
    port,
    extra_args,
    download_ttl_seconds
  );
  TemporalCoreDevServerOptions options;
  memset(&options, 0, sizeof(options));
  options.test_server = &test_options;
  options.namespace_ = temporal_mbt_bytes_ref(namespace_);
  options.ip = temporal_mbt_bytes_ref(ip);
  options.database_filename = temporal_mbt_bytes_ref(database_filename);
  options.ui = ui != 0;
  options.ui_port = (uint16_t)ui_port;
  options.log_format = temporal_mbt_bytes_ref(log_format);
  options.log_level = temporal_mbt_bytes_ref(log_level);

  TemporalMbtServerStartWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = runtime;
  temporal_core_ephemeral_server_start_dev_server(
    runtime->ptr,
    &options,
    &wait,
    temporal_mbt_server_start_callback
  );
  temporal_mbt_wait_finish(&wait.wait);
  return temporal_mbt_server_start_result(runtime, &wait);
}

typedef struct {
  TemporalMbtWait wait;
  TemporalMbtRuntime *runtime;
  uint8_t *message;
  size_t message_len;
} TemporalMbtServerShutdownWait;

static void temporal_mbt_server_shutdown_callback(void *user_data, const TemporalCoreByteArray *fail) {
  TemporalMbtServerShutdownWait *wait = (TemporalMbtServerShutdownWait *)user_data;
  if (fail != NULL) {
    temporal_mbt_copy_core_bytes(&wait->message, &wait->message_len, fail);
    temporal_core_byte_array_free(wait->runtime->ptr, fail);
  }
  temporal_mbt_wait_signal(&wait->wait);
}

MOONBIT_FFI_EXPORT
TemporalMbtCoreResult *temporal_mbt_ephemeral_server_shutdown(TemporalMbtEphemeralServer *server) {
  TemporalMbtServerShutdownWait wait;
  memset(&wait, 0, sizeof(wait));
  temporal_mbt_wait_init(&wait.wait);
  wait.runtime = server->runtime;
  temporal_core_ephemeral_server_shutdown(server->ptr, &wait, temporal_mbt_server_shutdown_callback);
  temporal_mbt_wait_finish(&wait.wait);
  TemporalMbtCoreResult *result = temporal_mbt_result_new();
  if (wait.message == NULL) {
    result->ok = true;
  } else {
    result->ok = false;
    result->message = wait.message;
    result->message_len = wait.message_len;
  }
  return result;
}
