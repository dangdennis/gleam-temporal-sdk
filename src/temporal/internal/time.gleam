//// Conversions between the time types of the public API and the milliseconds the bridge
//// to core counts in.

import gleam/option.{type Option}
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}

pub fn to_ms(duration: Duration) -> Int {
  duration.to_milliseconds(duration)
}

pub fn option_to_ms(duration: Option(Duration)) -> Option(Int) {
  option.map(duration, to_ms)
}

pub fn from_ms(ms: Int) -> Duration {
  duration.milliseconds(ms)
}

pub fn option_from_ms(ms: Option(Int)) -> Option(Duration) {
  option.map(ms, from_ms)
}

/// A point in time from milliseconds since the Unix epoch.
pub fn timestamp(ms: Int) -> Timestamp {
  timestamp.from_unix_seconds_and_nanoseconds(ms / 1000, ms % 1000 * 1_000_000)
}

pub fn timestamp_to_ms(timestamp: Timestamp) -> Int {
  let #(seconds, nanoseconds) =
    timestamp.to_unix_seconds_and_nanoseconds(timestamp)
  seconds * 1000 + nanoseconds / 1_000_000
}

/// A length of time as people write it, for messages: `1.5s`, `200ms`.
pub fn describe(duration: Duration) -> String {
  duration.to_iso8601_string(duration)
}
