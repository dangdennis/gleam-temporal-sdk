import gleeunit
import temporal/telemetry

@external(erlang, "temporal_test_ffi", "quiet_notices")
fn quiet_notices() -> Nil

pub fn main() -> Nil {
  // Every worker logs a notice when it starts and when it stops; the tests that are
  // about those lines capture them or read them from a node of their own.
  quiet_notices()
  // The tests provoke failed workflow tasks and replay mismatches by the dozen, and core
  // warns about each one.
  let assert Ok(Nil) = telemetry.set_core_log_level(telemetry.CoreError)
  gleeunit.main()
}
