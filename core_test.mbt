///|
test "cancellation token smoke" {
  let token = CancellationToken::new()
  token.cancel()
}

///|
test "random wrapper ranges and bytes" {
  let random = Random::new(7UL)
  let value = random.int32_range(10, 20)
  assert_true(value >= 10 && value < 20)
  let inclusive = random.int32_range(10, 10, max_inclusive=true)
  assert_eq(inclusive, 10)
  let d = random.double_range(0.0, 1.0)
  assert_true(d >= 0.0 && d < 1.0)
  let bytes = random.fill_bytes(16)
  assert_eq(bytes.length(), 16)
}

///|
test "runtime creation smoke" {
  let result = Runtime::new()
  guard result is Ok(runtime) else { fail("runtime creation failed") }
  let _ = runtime
}

///|
test "connection invalid options returns error" {
  let runtime_result = Runtime::new()
  guard runtime_result is Ok(runtime) else { fail("runtime creation failed") }
  let result = Connection::connect_blocking(
    runtime,
    ConnectionOptions::new("not a valid temporal url"),
  )
  guard result is Err(err) else { fail("expected invalid connection options") }
  assert_true(err.message.contains("Invalid options"))
}
