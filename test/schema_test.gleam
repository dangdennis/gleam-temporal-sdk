//// `temporal/schema`: one description of a JSON value gives both the encoder and the decoder.

import gleam/bit_array
import gleam/dict
import gleam/json
import gleam/option.{type Option, None, Some}
import temporal/codec
import temporal/schema.{type Schema}

type Address {
  Address(city: String, postcode: Option(String))
}

type Customer {
  Customer(
    name: String,
    age: Int,
    balance: Float,
    active: Bool,
    tags: List(String),
    address: Address,
    nickname: Option(String),
    scores: dict.Dict(String, Int),
  )
}

fn address() -> Schema(Address) {
  schema.object2(
    Address,
    schema.field("city", schema.string(), fn(a: Address) { a.city }),
    schema.field("postcode", schema.optional(schema.string()), fn(a: Address) {
      a.postcode
    }),
  )
}

fn customer() -> Schema(Customer) {
  schema.object8(
    Customer,
    schema.field("name", schema.string(), fn(c: Customer) { c.name }),
    schema.field("age", schema.int(), fn(c: Customer) { c.age }),
    schema.field("balance", schema.float(), fn(c: Customer) { c.balance }),
    schema.field("active", schema.bool(), fn(c: Customer) { c.active }),
    schema.field("tags", schema.list(schema.string()), fn(c: Customer) {
      c.tags
    }),
    schema.field("address", address(), fn(c: Customer) { c.address }),
    schema.field("nickname", schema.optional(schema.string()), fn(c: Customer) {
      c.nickname
    }),
    schema.field("scores", schema.dict(schema.int()), fn(c: Customer) {
      c.scores
    }),
  )
}

fn ada() -> Customer {
  Customer(
    name: "Ada",
    age: 36,
    balance: 12.5,
    active: True,
    tags: ["early", "vip"],
    address: Address(city: "London", postcode: Some("N1")),
    nickname: None,
    scores: dict.from_list([#("chess", 9)]),
  )
}

/// Decode JSON text with the schema's codec, as a payload from another SDK would be.
fn read(of: Schema(a), text: String) -> Result(a, String) {
  codec.decode(
    schema.codec(of),
    codec.Payload(
      metadata: dict.from_list([#("encoding", <<"json/plain":utf8>>)]),
      data: bit_array.from_string(text),
    ),
  )
}

/// Through a payload and back.
fn round_trip(of: Schema(a), value: a) -> Result(a, String) {
  let through = schema.codec(of)
  codec.decode(through, codec.encode(through, value))
}

pub fn primitives_round_trip_test() {
  assert round_trip(schema.string(), "héllo") == Ok("héllo")
  assert round_trip(schema.int(), -42) == Ok(-42)
  assert round_trip(schema.float(), 1.5) == Ok(1.5)
  assert round_trip(schema.bool(), True) == Ok(True)
  assert round_trip(schema.list(schema.int()), [1, 2, 3]) == Ok([1, 2, 3])
  assert round_trip(schema.optional(schema.int()), Some(1)) == Ok(Some(1))
  assert round_trip(schema.optional(schema.int()), None) == Ok(None)
  let ages = dict.from_list([#("ada", 36), #("bob", 41)])
  assert round_trip(schema.dict(schema.int()), ages) == Ok(ages)
}

pub fn a_nested_object_round_trips_test() {
  let through = schema.codec(customer())
  let payload = codec.encode(through, ada())
  assert dict.get(payload.metadata, "encoding") == Ok(<<"json/plain":utf8>>)
  assert codec.decode(through, payload) == Ok(ada())
}

pub fn an_object_is_written_under_its_field_names_test() {
  let london = Address(city: "London", postcode: Some("N1"))
  assert json.to_string(schema.to_json(address(), london))
    == "{\"city\":\"London\",\"postcode\":\"N1\"}"
  assert json.to_string(schema.to_json(
      address(),
      Address(city: "Paris", postcode: None),
    ))
    == "{\"city\":\"Paris\",\"postcode\":null}"
}

pub fn an_optional_field_may_be_null_or_absent_test() {
  let paris = Ok(Address(city: "Paris", postcode: None))
  assert read(address(), "{\"city\":\"Paris\",\"postcode\":null}") == paris
  assert read(address(), "{\"city\":\"Paris\"}") == paris
  // Fields the schema does not name are ignored.
  assert read(address(), "{\"city\":\"Paris\",\"country\":\"FR\"}") == paris
}

pub fn a_float_may_be_written_without_a_fraction_test() {
  assert read(schema.float(), "3") == Ok(3.0)
  assert read(schema.float(), "\"3\"") == Error("expected Float, found String")
}

pub fn the_decoder_works_with_gleam_json_directly_test() {
  assert json.parse("{\"city\":\"Oslo\"}", schema.decoder(address()))
    == Ok(Address(city: "Oslo", postcode: None))
}

pub fn every_object_size_round_trips_test() {
  let n = fn(name, index) {
    schema.field(name, schema.int(), fn(values: List(Int)) {
      case list_at(values, index) {
        Ok(value) -> value
        Error(Nil) -> -1
      }
    })
  }
  let check = fn(of, values) {
    assert round_trip(of, values) == Ok(values)
  }
  check(schema.object1(fn(a) { [a] }, n("a", 0)), [1])
  check(schema.object2(fn(a, b) { [a, b] }, n("a", 0), n("b", 1)), [1, 2])
  check(
    schema.object3(fn(a, b, c) { [a, b, c] }, n("a", 0), n("b", 1), n("c", 2)),
    [1, 2, 3],
  )
  check(
    schema.object4(
      fn(a, b, c, d) { [a, b, c, d] },
      n("a", 0),
      n("b", 1),
      n("c", 2),
      n("d", 3),
    ),
    [1, 2, 3, 4],
  )
  check(
    schema.object5(
      fn(a, b, c, d, e) { [a, b, c, d, e] },
      n("a", 0),
      n("b", 1),
      n("c", 2),
      n("d", 3),
      n("e", 4),
    ),
    [1, 2, 3, 4, 5],
  )
  check(
    schema.object6(
      fn(a, b, c, d, e, f) { [a, b, c, d, e, f] },
      n("a", 0),
      n("b", 1),
      n("c", 2),
      n("d", 3),
      n("e", 4),
      n("f", 5),
    ),
    [1, 2, 3, 4, 5, 6],
  )
  check(
    schema.object7(
      fn(a, b, c, d, e, f, g) { [a, b, c, d, e, f, g] },
      n("a", 0),
      n("b", 1),
      n("c", 2),
      n("d", 3),
      n("e", 4),
      n("f", 5),
      n("g", 6),
    ),
    [1, 2, 3, 4, 5, 6, 7],
  )
}

fn list_at(values: List(a), index: Int) -> Result(a, Nil) {
  case values, index {
    [value, ..], 0 -> Ok(value)
    [_, ..rest], _ -> list_at(rest, index - 1)
    [], _ -> Error(Nil)
  }
}

pub fn decode_errors_name_the_field_and_what_was_found_test() {
  assert read(address(), "{\"city\":7}")
    == Error("field \"city\": expected String, found Int")
  // A missing required field.
  assert read(address(), "{}")
    == Error("field \"city\": expected Field, found Nothing")
  // Nested fields are named by their path.
  let text =
    "{\"name\":\"Ada\",\"age\":36,\"balance\":12.5,\"active\":true,\"tags\":[],"
    <> "\"address\":{\"city\":\"London\",\"postcode\":5},\"scores\":{}}"
  assert read(customer(), text)
    == Error("field \"address.postcode\": expected String, found Int")
  assert read(address(), "{\"city\":") == Error("the JSON ends early")
}
