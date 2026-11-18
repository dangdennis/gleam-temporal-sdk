//// Payloads and the codecs that convert Gleam values to and from them.
////
//// Gleam has no runtime reflection, so every workflow, activity, signal and query declares
//// a codec for the values it sends. The JSON codecs produce `json/plain` payloads, the same
//// encoding the other Temporal SDKs use by default, so values interoperate across languages.

import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/dynamic/decode.{type Decoder}
import gleam/json.{type Json}
import gleam/result
import gleam/string

/// Temporal's unit of data on the wire: opaque bytes plus metadata naming their encoding.
pub type Payload {
  Payload(metadata: Dict(String, BitArray), data: BitArray)
}

pub type Codec(a) {
  Codec(encode: fn(a) -> Payload, decode: fn(Payload) -> Result(a, String))
}

/// A codec for values encoded as JSON.
pub fn json(to_json: fn(a) -> Json, decoder: Decoder(a)) -> Codec(a) {
  Codec(
    encode: fn(value) {
      Payload(
        metadata: dict.from_list([#("encoding", <<"json/plain":utf8>>)]),
        data: bit_array.from_string(json.to_string(to_json(value))),
      )
    },
    decode: fn(payload: Payload) {
      json.parse_bits(payload.data, decoder)
      |> result.map_error(fn(error) {
        "invalid JSON payload: " <> string.inspect(error)
      })
    },
  )
}

pub fn string() -> Codec(String) {
  json(json.string, decode.string)
}

pub fn int() -> Codec(Int) {
  json(json.int, decode.int)
}

pub fn float() -> Codec(Float) {
  json(json.float, decode.float)
}

pub fn bool() -> Codec(Bool) {
  json(json.bool, decode.bool)
}

/// A codec for `Nil`, encoded as Temporal's null payload. Any payload decodes to `Nil`.
pub fn nil() -> Codec(Nil) {
  Codec(
    encode: fn(_) {
      Payload(
        metadata: dict.from_list([#("encoding", <<"binary/null":utf8>>)]),
        data: <<>>,
      )
    },
    decode: fn(_) { Ok(Nil) },
  )
}

/// Decode the single argument Temporal delivers as a list of payloads. A missing argument
/// decodes as the null payload, so `nil()` inputs work when the caller sent nothing.
pub fn decode_argument(
  codec: Codec(a),
  payloads: List(Payload),
) -> Result(a, String) {
  case payloads {
    [payload, ..] -> codec.decode(payload)
    [] -> codec.decode(nil().encode(Nil))
  }
}

/// A codec for a JSON array of values.
pub fn list(to_json: fn(a) -> Json, decoder: Decoder(a)) -> Codec(List(a)) {
  json(json.array(_, to_json), decode.list(decoder))
}
