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

/// How to turn a value of one type into payloads and back. Definitions carry one for each
/// value they send, which is what makes a mismatch a compile error.
///
/// Most codecs are a `Codec`: one value, one payload. An `Arguments` codec covers a whole
/// argument list, for workflows and activities that take several arguments, as those
/// written with other SDKs often do: see `args2`.
pub type Codec(a) {
  Codec(encode: fn(a) -> Payload, decode: fn(Payload) -> Result(a, String))
  Arguments(
    encode: fn(a) -> List(Payload),
    decode: fn(List(Payload)) -> Result(a, String),
  )
}

/// The value as one payload. An argument-list codec gives its first argument.
pub fn encode(codec: Codec(a), value: a) -> Payload {
  case codec {
    Codec(encode:, ..) -> encode(value)
    Arguments(encode:, ..) ->
      case encode(value) {
        [first, ..] -> first
        [] -> null_payload()
      }
  }
}

/// The value one payload holds.
pub fn decode(codec: Codec(a), payload: Payload) -> Result(a, String) {
  case codec {
    Codec(decode:, ..) -> decode(payload)
    Arguments(decode:, ..) -> decode([payload])
  }
}

/// The value as an argument list: one payload, or one per argument for an `Arguments`
/// codec.
pub fn encode_arguments(codec: Codec(a), value: a) -> List(Payload) {
  case codec {
    Codec(encode:, ..) -> [encode(value)]
    Arguments(encode:, ..) -> encode(value)
  }
}

/// Decode an argument list. A single-value codec reads the first argument and ignores the
/// rest; with no arguments at all it reads a null, so `nil()` inputs work when the caller
/// sent nothing.
pub fn decode_arguments(
  codec: Codec(a),
  payloads: List(Payload),
) -> Result(a, String) {
  case codec, payloads {
    Arguments(decode:, ..), _ -> decode(payloads)
    Codec(decode:, ..), [payload, ..] -> decode(payload)
    Codec(decode:, ..), [] -> decode(null_payload())
  }
}

fn null_payload() -> Payload {
  Payload(
    metadata: dict.from_list([#("encoding", <<"binary/null":utf8>>)]),
    data: <<>>,
  )
}

/// A codec for a call with no arguments.
pub fn no_args() -> Codec(Nil) {
  Arguments(encode: fn(_) { [] }, decode: fn(_) { Ok(Nil) })
}

/// A codec for a call with two arguments, as a tuple.
pub fn args2(first: Codec(a), second: Codec(b)) -> Codec(#(a, b)) {
  Arguments(
    encode: fn(value: #(a, b)) {
      [encode(first, value.0), encode(second, value.1)]
    },
    decode: fn(payloads) {
      use a <- result.try(argument(first, payloads, 0))
      use b <- result.map(argument(second, payloads, 1))
      #(a, b)
    },
  )
}

/// A codec for a call with three arguments, as a tuple.
pub fn args3(
  first: Codec(a),
  second: Codec(b),
  third: Codec(c),
) -> Codec(#(a, b, c)) {
  Arguments(
    encode: fn(value: #(a, b, c)) {
      [encode(first, value.0), encode(second, value.1), encode(third, value.2)]
    },
    decode: fn(payloads) {
      use a <- result.try(argument(first, payloads, 0))
      use b <- result.try(argument(second, payloads, 1))
      use c <- result.map(argument(third, payloads, 2))
      #(a, b, c)
    },
  )
}

/// A codec for a call with four arguments, as a tuple.
pub fn args4(
  first: Codec(a),
  second: Codec(b),
  third: Codec(c),
  fourth: Codec(d),
) -> Codec(#(a, b, c, d)) {
  Arguments(
    encode: fn(value: #(a, b, c, d)) {
      [
        encode(first, value.0),
        encode(second, value.1),
        encode(third, value.2),
        encode(fourth, value.3),
      ]
    },
    decode: fn(payloads) {
      use a <- result.try(argument(first, payloads, 0))
      use b <- result.try(argument(second, payloads, 1))
      use c <- result.try(argument(third, payloads, 2))
      use d <- result.map(argument(fourth, payloads, 3))
      #(a, b, c, d)
    },
  )
}

/// The argument at `index`. A caller that sent fewer arguments sent nulls for the rest,
/// as far as the other SDKs are concerned.
fn argument(
  codec: Codec(a),
  payloads: List(Payload),
  index: Int,
) -> Result(a, String) {
  case list.drop(payloads, index) {
    [payload, ..] -> decode(codec, payload)
    [] -> decode(codec, null_payload())
  }
  |> result.map_error(fn(reason) {
    "argument " <> string.inspect(index + 1) <> ": " <> reason
  })
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

/// A codec for raw bytes, as the `binary/plain` payloads the other SDKs use for them.
pub fn bytes() -> Codec(BitArray) {
  Codec(
    encode: fn(value) {
      Payload(
        metadata: dict.from_list([#("encoding", <<"binary/plain":utf8>>)]),
        data: value,
      )
    },
    decode: fn(payload: Payload) { Ok(payload.data) },
  )
}

/// A codec for `Nil`, encoded as Temporal's null payload. Any payload decodes to `Nil`.
pub fn nil() -> Codec(Nil) {
  Codec(encode: fn(_) { null_payload() }, decode: fn(_) { Ok(Nil) })
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
