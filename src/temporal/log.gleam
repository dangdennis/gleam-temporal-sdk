//// Fields for structured log lines. `workflow.log_info` and `activity.log_info`, and their
//// siblings for the other levels, take a message and a list of fields; the fields become
//// metadata on the Erlang `logger` event, beside the identity of the workflow or activity
//// that logged it.
////
//// ```gleam
//// workflow.log_info(ctx, "order shipped", [
////   log.string("carrier", carrier),
////   log.int("parcels", 3),
//// ])
//// ```
////
//// Values keep their types: an int reaches the log handler as an integer, not as text.
//// Keys become atoms, as `logger` metadata keys are, so use a fixed set of keys written in
//// the code, never keys built from data. A field cannot replace the identity keys the SDK
//// sets itself, such as `workflow_id`.

import gleam/dynamic.{type Dynamic}
import gleam/list

/// One key and value to attach to a log line.
pub opaque type Field {
  Field(key: String, value: Dynamic)
}

/// A text field.
pub fn string(key: String, value: String) -> Field {
  Field(key, dynamic.string(value))
}

/// An integer field.
pub fn int(key: String, value: Int) -> Field {
  Field(key, dynamic.int(value))
}

/// A floating point field.
pub fn float(key: String, value: Float) -> Field {
  Field(key, dynamic.float(value))
}

/// A boolean field.
pub fn bool(key: String, value: Bool) -> Field {
  Field(key, dynamic.bool(value))
}

/// The fields as the metadata entries the logger is given.
@internal
pub fn metadata(fields: List(Field)) -> List(#(String, Dynamic)) {
  list.map(fields, fn(field) { #(field.key, field.value) })
}
