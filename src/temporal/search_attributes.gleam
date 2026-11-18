//// Search attributes: indexed values attached to a workflow, used to find it with
//// `client.list`. A custom attribute must be registered on the namespace before use.

import gleam/dict.{type Dict}
import gleam/json
import temporal/codec.{type Payload, Payload}

pub type Value {
  Keyword(String)
  Text(String)
  Int(Int)
  Float(Float)
  Bool(Bool)
  /// An RFC 3339 timestamp, such as `2026-01-31T12:00:00Z`.
  Datetime(String)
  KeywordList(List(String))
}

/// The wire form of a set of search attributes.
pub fn encode(attributes: Dict(String, Value)) -> Dict(String, Payload) {
  dict.map_values(attributes, fn(_, value) { encode_value(value) })
}

fn encode_value(value: Value) -> Payload {
  let #(kind, data) = case value {
    Keyword(text) -> #("Keyword", json.string(text))
    Text(text) -> #("Text", json.string(text))
    Int(number) -> #("Int", json.int(number))
    Float(number) -> #("Double", json.float(number))
    Bool(flag) -> #("Bool", json.bool(flag))
    Datetime(timestamp) -> #("Datetime", json.string(timestamp))
    KeywordList(items) -> #("KeywordList", json.array(items, json.string))
  }
  Payload(
    metadata: dict.from_list([
      #("encoding", <<"json/plain":utf8>>),
      #("type", <<kind:utf8>>),
    ]),
    data: <<json.to_string(data):utf8>>,
  )
}
