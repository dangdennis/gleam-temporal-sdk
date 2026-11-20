import gleeunit
import temporal/telemetry

pub fn main() -> Nil {
  // The tests provoke failed workflow tasks and replay mismatches by the dozen, and core
  // warns about each one.
  let assert Ok(Nil) = telemetry.set_core_log_level(telemetry.CoreError)
  gleeunit.main()
}
