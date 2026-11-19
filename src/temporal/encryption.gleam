//// A payload codec that encrypts with AES-256-GCM, in the format of the encryption codec in
//// Temporal's TypeScript, Go and Python samples, so that workers in those languages holding
//// the same key can read what this one writes.
////
//// An encrypted payload has the metadata `encoding: binary/encrypted` and
//// `encryption-key-id`, and its data is a 12-byte nonce followed by the ciphertext of the
//// original payload, serialized as protobuf, and the 16-byte authentication tag.
////
//// ```gleam
//// let codec = encryption.codec(encryption.Key(id: "2026-01", secret: key_bytes), [])
//// client.connect(client.Options(..client.local(), codec: Some(codec)))
//// ```
////
//// With encryption on, the Temporal UI and CLI show ciphertext. The key never leaves your
//// processes; anything that must read payloads needs it.

import gleam/bit_array
import gleam/dict
import gleam/list
import gleam/result
import temporal/codec.{type Payload, type PayloadCodec, Payload, PayloadCodec}

/// A 32-byte secret and the name payloads encrypted with it are labelled with.
pub type Key {
  Key(id: String, secret: BitArray)
}

const encoding = <<"binary/encrypted":utf8>>

/// Encrypt with `current`. Decrypt with `current` or any of `retired`, chosen by the key id
/// each payload carries: keep a rotated-out key in `retired` for as long as histories
/// written with it may be read. Payloads that are not encrypted pass through decoding
/// unchanged.
pub fn codec(current: Key, retired: List(Key)) -> PayloadCodec {
  let keys =
    dict.from_list(list.map([current, ..retired], fn(key) { #(key.id, key) }))
  PayloadCodec(
    encode: fn(payloads) { list.map(payloads, encrypt(current, _)) },
    decode: fn(payloads) {
      list.try_map(payloads, fn(payload) {
        case dict.get(payload.metadata, "encoding") {
          Ok(found) if found == encoding -> decrypt(keys, payload)
          _ -> Ok(payload)
        }
      })
    },
  )
}

fn encrypt(key: Key, payload: Payload) -> Payload {
  Payload(
    metadata: dict.from_list([
      #("encoding", encoding),
      #("encryption-key-id", bit_array.from_string(key.id)),
    ]),
    data: aes_gcm_encrypt(key.secret, codec.to_bytes(payload)),
  )
}

fn decrypt(
  keys: dict.Dict(String, Key),
  payload: Payload,
) -> Result(Payload, String) {
  let key_id =
    dict.get(payload.metadata, "encryption-key-id")
    |> result.try(bit_array.to_string)
    |> result.unwrap("")
  case dict.get(keys, key_id) {
    Error(Nil) -> Error("no encryption key with id \"" <> key_id <> "\"")
    Ok(key) ->
      aes_gcm_decrypt(key.secret, payload.data)
      |> result.replace_error(
        "payload could not be decrypted with key \"" <> key_id <> "\"",
      )
      |> result.try(codec.from_bytes)
  }
}

@external(erlang, "temporal_ffi", "aes_gcm_encrypt")
fn aes_gcm_encrypt(key: BitArray, plaintext: BitArray) -> BitArray

@external(erlang, "temporal_ffi", "aes_gcm_decrypt")
fn aes_gcm_decrypt(key: BitArray, data: BitArray) -> Result(BitArray, Nil)
