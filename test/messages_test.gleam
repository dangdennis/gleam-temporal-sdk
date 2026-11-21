//// Error messages a user is likely to meet say what went wrong and where.

import gleam/dict
import gleam/dynamic/decode
import gleam/json
import gleam/string
import temporal/codec
import temporal/internal/bridge
import temporal/internal/runtime/run_state

pub fn json_decode_error_names_the_field_test() {
  let deleted =
    codec.json(fn(count) { json.object([#("deleted", json.int(count))]) }, {
      use count <- decode.field("deleted", decode.int)
      decode.success(count)
    })
  let payload =
    codec.Payload(dict.from_list([#("encoding", <<"json/plain":utf8>>)]), <<
      "{\"deleted\":\"three\"}":utf8,
    >>)
  assert codec.decode(deleted, payload)
    == Error("field \"deleted\": expected Int, found String")
}

pub fn workflow_function_outside_a_workflow_says_so_test() {
  let assert Error(#(summary, _, _)) = bridge.rescue(fn() { run_state.get() })
  assert string.contains(summary, "works only in workflow code")
}
