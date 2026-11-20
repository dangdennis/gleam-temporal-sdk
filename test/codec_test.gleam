//// What the JSON codecs make of payloads other SDKs write: a missing value, numbers the
//// way JavaScript prints them, and text that is not quite text.

import gleam/bit_array
import gleam/dict
import gleam/dynamic/decode
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/string
import temporal/codec.{type Payload, Payload}
import temporal/schema

/// What TypeScript sends for `undefined`, and every SDK for an argument left out.
fn binary_null() -> Payload {
  Payload(dict.from_list([#("encoding", <<"binary/null":utf8>>)]), <<>>)
}

fn json_payload(text: BitArray) -> Payload {
  Payload(dict.from_list([#("encoding", <<"json/plain":utf8>>)]), text)
}

type Note {
  Note(title: String, body: Option(String))
}

fn note() -> schema.Schema(Note) {
  schema.object2(
    Note,
    schema.field("title", schema.string(), fn(n: Note) { n.title }),
    schema.field("body", schema.optional(schema.string()), fn(n: Note) {
      n.body
    }),
  )
}

pub fn a_null_payload_reads_as_json_null_test() {
  let optional = schema.codec(schema.optional(schema.string()))
  assert codec.decode(optional, binary_null()) == Ok(None)
  assert codec.decode(optional, json_payload(<<"null":utf8>>)) == Ok(None)
  assert codec.decode(optional, json_payload(<<"\"x\"":utf8>>)) == Ok(Some("x"))
  assert codec.decode(schema.codec(schema.optional(note())), binary_null())
    == Ok(None)
  // A hand-written codec gets the same null.
  let by_hand =
    codec.json(json.nullable(_, json.int), decode.optional(decode.int))
  assert codec.decode(by_hand, binary_null()) == Ok(None)
}

pub fn a_null_payload_where_a_value_is_needed_says_what_was_expected_test() {
  assert codec.decode(codec.string(), binary_null())
    == Error("expected String, found Nil")
  assert codec.decode(codec.int(), binary_null())
    == Error("expected Int, found Nil")
  let assert Error(message) = codec.decode(schema.codec(note()), binary_null())
  assert string.starts_with(message, "expected Dict, found Nil")
}

pub fn nil_reads_every_way_of_sending_nothing_test() {
  assert codec.decode(codec.nil(), binary_null()) == Ok(Nil)
  assert codec.decode(codec.nil(), json_payload(<<"null":utf8>>)) == Ok(Nil)
  assert codec.decode_arguments(codec.nil(), []) == Ok(Nil)
  assert codec.encode(codec.nil(), Nil) == binary_null()
}

pub fn an_argument_left_out_reads_as_null_test() {
  let arguments =
    codec.args3(
      codec.string(),
      schema.codec(schema.optional(schema.int())),
      schema.codec(schema.optional(note())),
    )
  let hey = codec.encode(codec.string(), "hey")
  assert codec.decode_arguments(arguments, [hey]) == Ok(#("hey", None, None))
  assert codec.decode_arguments(arguments, [hey, binary_null(), binary_null()])
    == Ok(#("hey", None, None))
  assert codec.decode_arguments(arguments, [
      hey,
      codec.encode(codec.int(), 2),
      json_payload(<<"{\"title\":\"t\"}":utf8>>),
    ])
    == Ok(#("hey", Some(2), Some(Note("t", None))))
  // One that is needed and missing is named.
  assert codec.decode_arguments(codec.args2(codec.string(), codec.int()), [hey])
    == Error("argument 2: expected Int, found Nil")
  // What `args2` writes for a missing argument, it reads back.
  let pair = codec.args2(codec.string(), codec.nil())
  assert codec.decode_arguments(pair, codec.encode_arguments(pair, #("a", Nil)))
    == Ok(#("a", Nil))
  // A single-value codec given no arguments at all reads a null too.
  assert codec.decode_arguments(schema.codec(schema.optional(note())), [])
    == Ok(None)
  assert codec.decode_arguments(codec.string(), [])
    == Error("expected String, found Nil")
}

pub fn a_float_reads_a_whole_number_test() {
  assert codec.decode(codec.float(), json_payload(<<"10":utf8>>)) == Ok(10.0)
  assert codec.decode(codec.float(), json_payload(<<"-3":utf8>>)) == Ok(-3.0)
  assert codec.decode(codec.float(), json_payload(<<"2.5":utf8>>)) == Ok(2.5)
  assert codec.decode(codec.float(), json_payload(<<"1e3":utf8>>)) == Ok(1000.0)
  assert codec.decode(schema.codec(schema.float()), json_payload(<<"10":utf8>>))
    == Ok(10.0)
  assert codec.decode(codec.float(), json_payload(<<"\"10\"":utf8>>))
    == Error("expected Float, found String")
}

pub fn an_int_reads_a_whole_number_however_it_is_written_test() {
  let read = fn(text) { codec.decode(codec.int(), json_payload(<<text:utf8>>)) }
  assert read("42") == Ok(42)
  // JavaScript prints numbers from 1e21 up with an exponent.
  assert read("1e21") == Ok(1_000_000_000_000_000_000_000)
  assert read("1E3") == Ok(1000)
  assert read("-2.5e1") == Ok(-25)
  assert read("7.0") == Ok(7)
  assert read("12345678901234567890123") == Ok(12_345_678_901_234_567_890_123)
  assert read("2.5") == Error("expected Int, found Float")
  assert read("1e-3") == Error("expected Int, found Float")
  assert codec.decode(schema.codec(schema.int()), json_payload(<<"1e21":utf8>>))
    == Ok(1_000_000_000_000_000_000_000)
}

pub fn a_lone_surrogate_reads_as_the_replacement_character_test() {
  let read = fn(text) {
    codec.decode(codec.string(), json_payload(<<text:utf8>>))
  }
  // A JavaScript string cut through an emoji ends in half a surrogate pair.
  assert read("\"cut \\ud83d\"") == Ok("cut \u{FFFD}")
  assert read("\"\\ude00 tail\"") == Ok("\u{FFFD} tail")
  assert read("\"\\uD83D\\u0041\"") == Ok("\u{FFFD}A")
  assert read("\"\\ud83d\\ud83d\\ude00\"") == Ok("\u{FFFD}\u{1F600}")
  // A whole pair is left alone, and so is a backslash that only looks like an escape.
  assert read("\"\\ud83d\\ude00\"") == Ok("\u{1F600}")
  assert read("\"\\\\ud83d\"") == Ok("\\ud83d")
  assert codec.decode(
      schema.codec(note()),
      json_payload(<<"{\"title\":\"\\ud83d\",\"body\":null}":utf8>>),
    )
    == Ok(Note("\u{FFFD}", None))
}

pub fn bytes_that_are_not_utf8_are_an_error_test() {
  let read = fn(bytes) { codec.decode(codec.string(), json_payload(bytes)) }
  let assert Error(_) = read(<<"\"a":utf8, 0xFF, "b\"":utf8>>)
  let assert Error(_) = read(<<0xFF, 0xFE>>)
  // A multi-byte character cut short.
  let assert Error(_) = read(<<"\"":utf8, 0xE2, 0x82, "\"":utf8>>)
  let assert Error(_) =
    codec.decode(
      schema.codec(note()),
      json_payload(<<"{\"title\":\"":utf8, 0xC3, 0x28, "\"}":utf8>>),
    )
  // The message quotes the input, and must itself be text: it travels in a failure.
  let assert Error(message) = read(<<"\"\\u00":utf8, 0xFF, "\"":utf8>>)
  assert string.starts_with(message, "not valid JSON: unexpected ")
  assert bit_array.is_utf8(bit_array.from_string(message))
  // Payload metadata too.
  assert codec.from_bytes(<<10, 6, 10, 2, 0xFF, 0xFE, 18, 0>>)
    == Error("payload metadata key is not UTF-8")
}
