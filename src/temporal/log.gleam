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
//// The fields travel in one map under the metadata key `fields`, keyed by the field's
//// name as a binary: `#{temporal => true, workflow_id => <<"order-1">>, ..., fields =>
//// #{<<"carrier">> => <<"dhl">>, <<"parcels">> => 3}}`. Values keep their types: an int
//// reaches the log handler as an integer, not as text. The names are never made into
//// atoms, so a key built from data cannot fill the VM's atom table, and a field cannot
//// replace the identity keys the SDK sets itself, such as `workflow_id`, which stay atoms
//// at the top level. A formatter or filter reads a field from the map:
////
//// ```erlang
//// parcels(#{meta := #{fields := #{<<"parcels">> := Parcels}}}) -> Parcels;
//// parcels(_) -> undefined.
//// ```
////
//// A line logged with no fields has no `fields` key. `logger_formatter`'s template takes
//// atom keys only, so it can print the whole map with `fields` and the identity keys by
//// name, but not one field.
////
//// The lines reach `logger`'s default handler as it comes. `logger` starts at level
//// `notice`, so info and debug lines show once the level is lowered, for instance with
//// `logger:set_primary_config(level, info)`. Every line the SDK logs, these and the
//// worker's own warnings and errors, carries the metadata `temporal => true` and no
//// `domain`, so a handler or filter can pick the SDK's lines out by that key:
////
//// ```erlang
//// logger:add_handler_filter(default, no_temporal, {fun
////     (#{meta := #{temporal := true}}, _) -> stop;
////     (_, _) -> ignore
//// end, []}).
//// ```

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

/// The fields as the entries of the `fields` map the logger is given.
@internal
pub fn metadata(fields: List(Field)) -> List(#(String, Dynamic)) {
  list.map(fields, fn(field) { #(field.key, field.value) })
}
