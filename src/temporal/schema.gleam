//// Describe a JSON value once and get both directions: the encoder and the decoder come
//// from the same description, so they cannot drift apart. Schemas compose, so objects
//// nest.
////
//// ```gleam
//// pub type Output {
////   Output(deleted_otps: Int, deleted_sessions: Int)
//// }
////
//// fn output_schema() -> schema.Schema(Output) {
////   schema.object2(
////     Output,
////     schema.field("deletedOtps", schema.int(), fn(o: Output) { o.deleted_otps }),
////     schema.field("deletedSessions", schema.int(), fn(o: Output) { o.deleted_sessions }),
////   )
//// }
////
//// pub fn cleanup() {
////   workflow.Definition("cleanup", codec.nil(), schema.codec(output_schema()))
//// }
//// ```
////
//// The fields are given in the order of the constructor's arguments. For JSON that an
//// object schema cannot describe, such as a custom type with several variants, write the
//// two directions by hand and use `codec.json`.

import gleam/dict.{type Dict}
import gleam/dynamic/decode.{type Decoder}
import gleam/int
import gleam/json.{type Json}
import gleam/option.{type Option, None, Some}
import temporal/codec.{type Codec}

/// How a value of one type is written as JSON and read back.
pub opaque type Schema(a) {
  Schema(
    to_json: fn(a) -> Json,
    decoder: Decoder(a),
    /// The value an object field of this schema has when the field is absent, if it may be.
    absent: Option(a),
  )
}

/// One field of a JSON object: its name, the schema of its value, and how to read the
/// value out of the record the object stands for.
pub opaque type Field(record, value) {
  Field(name: String, schema: Schema(value), get: fn(record) -> value)
}

/// A JSON string.
pub fn string() -> Schema(String) {
  Schema(json.string, decode.string, None)
}

/// A JSON integer.
pub fn int() -> Schema(Int) {
  Schema(json.int, decode.int, None)
}

/// A JSON number, read as a float. A number written without a fraction, such as `3`, is
/// read too, since other SDKs write whole floats that way.
pub fn float() -> Schema(Float) {
  Schema(
    json.float,
    decode.one_of(decode.float, [decode.map(decode.int, int.to_float)]),
    None,
  )
}

/// A JSON boolean.
pub fn bool() -> Schema(Bool) {
  Schema(json.bool, decode.bool, None)
}

/// A JSON array whose elements all follow one schema.
pub fn list(of element: Schema(a)) -> Schema(List(a)) {
  Schema(json.array(_, element.to_json), decode.list(element.decoder), None)
}

/// A value that may be missing. `None` is written as JSON null; null reads as `None`, and
/// so does an object field that is not there at all.
pub fn optional(of inner: Schema(a)) -> Schema(Option(a)) {
  Schema(
    json.nullable(_, inner.to_json),
    decode.optional(inner.decoder),
    Some(None),
  )
}

/// A JSON object used as a map from strings to values of one schema.
pub fn dict(of value: Schema(a)) -> Schema(Dict(String, a)) {
  Schema(
    json.dict(_, fn(key) { key }, value.to_json),
    decode.dict(decode.string, value.decoder),
    None,
  )
}

/// A field of an object, for `object1` to `object8`. `get` reads the field's value from
/// the record.
pub fn field(
  name: String,
  schema: Schema(value),
  get: fn(record) -> value,
) -> Field(record, value) {
  Field(name:, schema:, get:)
}

/// A codec that sends the value as a `json/plain` payload, for workflow, activity, signal,
/// query and update definitions.
pub fn codec(schema: Schema(a)) -> Codec(a) {
  codec.json(schema.to_json, schema.decoder)
}

/// The value as JSON, for use with `gleam/json` directly.
pub fn to_json(schema: Schema(a), value: a) -> Json {
  schema.to_json(value)
}

/// The schema's decoder, for use with `gleam/dynamic/decode` directly.
pub fn decoder(schema: Schema(a)) -> Decoder(a) {
  schema.decoder
}

fn entry(field: Field(record, value), record: record) -> #(String, Json) {
  #(field.name, field.schema.to_json(field.get(record)))
}

fn read(
  field: Field(record, value),
  next: fn(value) -> Decoder(final),
) -> Decoder(final) {
  case field.schema.absent {
    Some(default) ->
      decode.optional_field(field.name, default, field.schema.decoder, next)
    None -> decode.field(field.name, field.schema.decoder, next)
  }
}

/// A JSON object with one field. `constructor` builds the record from the fields' values, in
/// the order the fields are given.
pub fn object1(
  constructor: fn(a) -> record,
  f1: Field(record, a),
) -> Schema(record) {
  Schema(
    fn(record) { json.object([entry(f1, record)]) },
    {
      use a <- read(f1)
      decode.success(constructor(a))
    },
    None,
  )
}

/// A JSON object with two fields. See `object1`.
pub fn object2(
  constructor: fn(a, b) -> record,
  f1: Field(record, a),
  f2: Field(record, b),
) -> Schema(record) {
  Schema(
    fn(record) { json.object([entry(f1, record), entry(f2, record)]) },
    {
      use a <- read(f1)
      use b <- read(f2)
      decode.success(constructor(a, b))
    },
    None,
  )
}

/// A JSON object with three fields. See `object1`.
pub fn object3(
  constructor: fn(a, b, c) -> record,
  f1: Field(record, a),
  f2: Field(record, b),
  f3: Field(record, c),
) -> Schema(record) {
  Schema(
    fn(record) {
      json.object([entry(f1, record), entry(f2, record), entry(f3, record)])
    },
    {
      use a <- read(f1)
      use b <- read(f2)
      use c <- read(f3)
      decode.success(constructor(a, b, c))
    },
    None,
  )
}

/// A JSON object with four fields. See `object1`.
pub fn object4(
  constructor: fn(a, b, c, d) -> record,
  f1: Field(record, a),
  f2: Field(record, b),
  f3: Field(record, c),
  f4: Field(record, d),
) -> Schema(record) {
  Schema(
    fn(record) {
      json.object([
        entry(f1, record),
        entry(f2, record),
        entry(f3, record),
        entry(f4, record),
      ])
    },
    {
      use a <- read(f1)
      use b <- read(f2)
      use c <- read(f3)
      use d <- read(f4)
      decode.success(constructor(a, b, c, d))
    },
    None,
  )
}

/// A JSON object with five fields. See `object1`.
pub fn object5(
  constructor: fn(a, b, c, d, e) -> record,
  f1: Field(record, a),
  f2: Field(record, b),
  f3: Field(record, c),
  f4: Field(record, d),
  f5: Field(record, e),
) -> Schema(record) {
  Schema(
    fn(record) {
      json.object([
        entry(f1, record),
        entry(f2, record),
        entry(f3, record),
        entry(f4, record),
        entry(f5, record),
      ])
    },
    {
      use a <- read(f1)
      use b <- read(f2)
      use c <- read(f3)
      use d <- read(f4)
      use e <- read(f5)
      decode.success(constructor(a, b, c, d, e))
    },
    None,
  )
}

/// A JSON object with six fields. See `object1`.
pub fn object6(
  constructor: fn(a, b, c, d, e, f) -> record,
  f1: Field(record, a),
  f2: Field(record, b),
  f3: Field(record, c),
  f4: Field(record, d),
  f5: Field(record, e),
  f6: Field(record, f),
) -> Schema(record) {
  Schema(
    fn(record) {
      json.object([
        entry(f1, record),
        entry(f2, record),
        entry(f3, record),
        entry(f4, record),
        entry(f5, record),
        entry(f6, record),
      ])
    },
    {
      use a <- read(f1)
      use b <- read(f2)
      use c <- read(f3)
      use d <- read(f4)
      use e <- read(f5)
      use f <- read(f6)
      decode.success(constructor(a, b, c, d, e, f))
    },
    None,
  )
}

/// A JSON object with seven fields. See `object1`.
pub fn object7(
  constructor: fn(a, b, c, d, e, f, g) -> record,
  f1: Field(record, a),
  f2: Field(record, b),
  f3: Field(record, c),
  f4: Field(record, d),
  f5: Field(record, e),
  f6: Field(record, f),
  f7: Field(record, g),
) -> Schema(record) {
  Schema(
    fn(record) {
      json.object([
        entry(f1, record),
        entry(f2, record),
        entry(f3, record),
        entry(f4, record),
        entry(f5, record),
        entry(f6, record),
        entry(f7, record),
      ])
    },
    {
      use a <- read(f1)
      use b <- read(f2)
      use c <- read(f3)
      use d <- read(f4)
      use e <- read(f5)
      use f <- read(f6)
      use g <- read(f7)
      decode.success(constructor(a, b, c, d, e, f, g))
    },
    None,
  )
}

/// A JSON object with eight fields. See `object1`.
pub fn object8(
  constructor: fn(a, b, c, d, e, f, g, h) -> record,
  f1: Field(record, a),
  f2: Field(record, b),
  f3: Field(record, c),
  f4: Field(record, d),
  f5: Field(record, e),
  f6: Field(record, f),
  f7: Field(record, g),
  f8: Field(record, h),
) -> Schema(record) {
  Schema(
    fn(record) {
      json.object([
        entry(f1, record),
        entry(f2, record),
        entry(f3, record),
        entry(f4, record),
        entry(f5, record),
        entry(f6, record),
        entry(f7, record),
        entry(f8, record),
      ])
    },
    {
      use a <- read(f1)
      use b <- read(f2)
      use c <- read(f3)
      use d <- read(f4)
      use e <- read(f5)
      use f <- read(f6)
      use g <- read(f7)
      use h <- read(f8)
      decode.success(constructor(a, b, c, d, e, f, g, h))
    },
    None,
  )
}
