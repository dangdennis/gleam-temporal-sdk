//// Payloads and the codecs that convert Gleam values to and from them.
////
//// Gleam has no runtime reflection, so every workflow, activity, signal and query declares
//// a codec for the values it sends. The JSON codecs produce `json/plain` payloads, the same
//// encoding the other Temporal SDKs use by default, so values interoperate across languages.

import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/dynamic/decode.{type Decoder}
import gleam/json.{type Json}
import gleam/list
import gleam/result
import gleam/string

/// Temporal's unit of data on the wire: opaque bytes plus metadata naming their encoding.
pub type Payload {
  Payload(metadata: Dict(String, BitArray), data: BitArray)
}

/// How to turn a value of one type into a payload and back. Definitions carry one for
/// each value they send, which is what makes a mismatch a compile error.
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

/// A codec for a JSON string.
pub fn string() -> Codec(String) {
  json(json.string, decode.string)
}

/// A codec for a JSON integer.
pub fn int() -> Codec(Int) {
  json(json.int, decode.int)
}

/// A codec for a JSON number.
pub fn float() -> Codec(Float) {
  json(json.float, decode.float)
}

/// A codec for a JSON boolean.
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

// ---- payload codecs ----

/// A transformation applied to every payload on its way to and from the Temporal service:
/// workflow, activity, signal, query and update arguments and results, memo values,
/// heartbeat details and failure details. Use one to encrypt or compress what the service
/// stores. Set it in `client.Options`; workers use their client's.
///
/// Search attributes are not transformed, because the service has to index them.
///
/// A codec runs outside workflow code, so it may be non-deterministic and may do IO, such
/// as fetching a key. `decode` must accept payloads the codec did not produce and return
/// them unchanged, so that data written before the codec was introduced stays readable.
pub type PayloadCodec {
  PayloadCodec(
    encode: fn(List(Payload)) -> List(Payload),
    decode: fn(List(Payload)) -> Result(List(Payload), String),
  )
}

/// Run `outer` around `inner`: payloads are encoded by `inner` first, such as compression,
/// then by `outer`, such as encryption, and decoded in the opposite order.
pub fn compose(outer: PayloadCodec, inner: PayloadCodec) -> PayloadCodec {
  PayloadCodec(
    encode: fn(payloads) { outer.encode(inner.encode(payloads)) },
    decode: fn(payloads) { result.try(outer.decode(payloads), inner.decode) },
  )
}

/// The payload as a serialized `temporal.api.common.v1.Payload` protobuf message, which is
/// what codecs in the other SDKs conventionally encrypt or compress.
pub fn to_bytes(payload: Payload) -> BitArray {
  let metadata =
    dict.to_list(payload.metadata)
    |> list.sort(fn(a, b) { string.compare(a.0, b.0) })
    |> list.map(fn(entry) {
      let entry =
        bit_array.concat([
          field(1, bit_array.from_string(entry.0)),
          field(2, entry.1),
        ])
      field(1, entry)
    })
  bit_array.concat(
    list.append(metadata, case payload.data {
      <<>> -> []
      data -> [field(2, data)]
    }),
  )
}

/// Parse a serialized `temporal.api.common.v1.Payload`.
pub fn from_bytes(bytes: BitArray) -> Result(Payload, String) {
  parse_payload(bytes, Payload(dict.new(), <<>>))
}

/// A length-delimited protobuf field.
fn field(number: Int, value: BitArray) -> BitArray {
  bit_array.concat([
    <<{ number * 8 + 2 }>>,
    varint(bit_array.byte_size(value)),
    value,
  ])
}

fn varint(value: Int) -> BitArray {
  case value < 128 {
    True -> <<value>>
    False -> bit_array.concat([<<{ value % 128 + 128 }>>, varint(value / 128)])
  }
}

fn read_varint(
  bytes: BitArray,
  shift: Int,
  total: Int,
) -> Result(#(Int, BitArray), String) {
  case bytes {
    <<1:1, low:7, rest:bytes>> ->
      read_varint(rest, shift * 128, total + low * shift)
    <<0:1, low:7, rest:bytes>> -> Ok(#(total + low * shift, rest))
    _ -> Error("truncated payload")
  }
}

/// Read one length-delimited field: its number, its value and the bytes after it.
fn read_field(bytes: BitArray) -> Result(#(Int, BitArray, BitArray), String) {
  use #(tag, rest) <- result.try(read_varint(bytes, 1, 0))
  case tag % 8 {
    2 -> {
      use #(length, rest) <- result.try(read_varint(rest, 1, 0))
      case rest {
        <<value:bytes-size(length), rest:bytes>> -> Ok(#(tag / 8, value, rest))
        _ -> Error("truncated payload")
      }
    }
    _ -> Error("unexpected field type in payload")
  }
}

fn parse_payload(bytes: BitArray, payload: Payload) -> Result(Payload, String) {
  case bytes {
    <<>> -> Ok(payload)
    _ -> {
      use #(number, value, rest) <- result.try(read_field(bytes))
      case number {
        1 -> {
          use #(key, entry) <- result.try(parse_entry(value, #("", <<>>)))
          parse_payload(
            rest,
            Payload(
              ..payload,
              metadata: dict.insert(payload.metadata, key, entry),
            ),
          )
        }
        2 -> parse_payload(rest, Payload(..payload, data: value))
        // Fields this SDK does not use.
        _ -> parse_payload(rest, payload)
      }
    }
  }
}

fn parse_entry(
  bytes: BitArray,
  entry: #(String, BitArray),
) -> Result(#(String, BitArray), String) {
  case bytes {
    <<>> -> Ok(entry)
    _ -> {
      use #(number, value, rest) <- result.try(read_field(bytes))
      case number, bit_array.to_string(value) {
        1, Ok(key) -> parse_entry(rest, #(key, entry.1))
        1, Error(Nil) -> Error("payload metadata key is not UTF-8")
        _, _ -> parse_entry(rest, #(entry.0, value))
      }
    }
  }
}
