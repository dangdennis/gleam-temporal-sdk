//// A payload codec that encrypts with AES-256-GCM, in the format of the encryption codec in
//// Temporal's TypeScript, Go and Python samples, so that workers in those languages holding
//// the same key can read what this one writes.
////
//// An encrypted payload has the metadata `encoding: binary/encrypted` and
//// `encryption-key-id`, and its data is a 12-byte nonce followed by the ciphertext of the
//// original payload, serialized as protobuf, and the 16-byte authentication tag.
////
//// ```gleam
//// let assert Ok(key) = encryption.key(id: "2026-01", secret: key_bytes)
//// client.connect(
////   client.local()
////   |> client.with_codec(encryption.codec(key, []))
////   |> client.with_encoded_failure_attributes,
//// )
//// ```
////
//// With encryption on, the Temporal UI and CLI show ciphertext. The key never leaves your
//// processes; anything that must read payloads needs it.
////
//// ## What is encrypted and what is not
////
//// A payload codec sees payloads, and Temporal stores a good deal beside them.
////
//// | Encrypted | Not encrypted |
//// |---|---|
//// | Workflow, activity, child workflow and Nexus operation arguments and results | Search attributes: the server indexes them, so it must read them |
//// | Signal, query and update arguments and results | Workflow and activity type names; signal, query and update names |
//// | Memo values | Workflow, run, activity and update ids; task queue and namespace names |
//// | Activity heartbeat details, continue-as-new arguments | Headers (see below) |
//// | The details attached to a failure | A failure's message and stack trace, unless `client.with_encoded_failure_attributes` is on |
//// | With `client.with_encoded_failure_attributes`: every failure's message and stack trace | A failure's type, its non-retryable flag, timeouts, retry policies, cron and schedule timetables |
////
//// So put nothing sensitive in an id, a name, a search attribute or a header: a customer's
//// email address makes a poor workflow id.
////
//// Failure messages need the option because they are text, not payloads. When workflow or
//// activity code crashes, the SDK's own message names the kind of error and where it
//// happened and leaves out the values involved, but a message your code writes with
//// `failure.new` says whatever you put in it. Turn the option on in every client and
//// worker of a namespace that encrypts.
////
//// Headers are not passed through the codec, which is what Temporal's TypeScript SDK does
//// by default: it applies codecs to headers only for workers that set its experimental
//// `codecHeaders` option, and never to the headers a client sends. A header one SDK
//// encrypted and another did not decrypt would break the workflows they share, so this
//// SDK leaves them as they are. Use headers for context such as a trace id, not for
//// anything that must stay private.
////
//// ## Strict decoding
////
//// A payload that is not encrypted passes through decoding unchanged, as it does in
//// Temporal's samples, so that a namespace can turn encryption on while histories written
//// before it are still being read. It also means that whoever can write to the namespace
//// can hand a worker plaintext that it will accept. Once every history in the namespace
//// that will still be read was written with encryption, wrap the codec in `strict` to
//// refuse anything else:
////
//// ```gleam
//// encryption.codec(key, [old_key]) |> encryption.strict
//// ```

import gleam/bit_array
import gleam/dict
import gleam/int
import gleam/list
import gleam/result
import temporal/codec.{type Payload, type PayloadCodec, Payload, PayloadCodec}
import temporal/internal/secret.{type Secret}

/// A 32-byte secret and the name payloads encrypted with it are labelled with. Make one
/// with `key`. Printing a key shows its id and not its secret.
pub opaque type Key {
  Key(id: String, secret: Secret(BitArray))
}

/// A key for AES-256-GCM. `id` is stored in the clear beside every payload encrypted with
/// the key and chooses the key to decrypt with, so it names the key and says nothing of
/// it. `secret` is the key itself and must be exactly 32 bytes: anything else is an error
/// here, not at the first payload.
pub fn key(id id: String, secret secret: BitArray) -> Result(Key, String) {
  case bit_array.byte_size(secret), bit_array.bit_size(secret) % 8 {
    32, 0 -> Ok(Key(id:, secret: secret.new(secret)))
    size, _ ->
      Error(
        "the secret of encryption key \""
        <> id
        <> "\" is "
        <> int.to_string(size)
        <> " bytes: AES-256 takes a key of exactly 32 bytes",
      )
  }
}

const encoding = <<"binary/encrypted":utf8>>

/// Encrypt with `current`. Decrypt with `current` or any of `retired`, chosen by the key id
/// each payload carries: keep a rotated-out key in `retired` for as long as histories
/// written with it may be read. Payloads that are not encrypted pass through decoding
/// unchanged; `strict` refuses them.
pub fn codec(current: Key, retired: List(Key)) -> PayloadCodec {
  let keys =
    dict.from_list(list.map([current, ..retired], fn(key) { #(key.id, key) }))
  PayloadCodec(
    encode: fn(payloads) { list.map(payloads, encrypt(current, _)) },
    decode: fn(payloads) {
      list.try_map(payloads, fn(payload) {
        case is_encrypted(payload) {
          True -> decrypt(keys, payload)
          False -> Ok(payload)
        }
      })
    },
  )
}

/// The codec, refusing to decode a payload that is not encrypted: decoding fails with an
/// error instead of passing the payload through. A payload encrypted with a key the codec
/// does not hold is refused either way.
///
/// Turn it on once every history in the namespace that will still be read was written
/// with encryption. Until then leave it off: a workflow started before encryption could
/// not be replayed, and its tasks would fail until the codec is relaxed again.
pub fn strict(codec: PayloadCodec) -> PayloadCodec {
  PayloadCodec(encode: codec.encode, decode: fn(payloads) {
    case list.all(payloads, is_encrypted) {
      True -> codec.decode(payloads)
      False ->
        Error(
          "refusing a payload that is not encrypted: the encryption codec is "
          <> "strict, and this payload has no `binary/encrypted` encoding. It was "
          <> "written without the codec, or by someone without a key",
        )
    }
  })
}

fn is_encrypted(payload: Payload) -> Bool {
  dict.get(payload.metadata, "encoding") == Ok(encoding)
}

fn encrypt(key: Key, payload: Payload) -> Payload {
  Payload(
    metadata: dict.from_list([
      #("encoding", encoding),
      #("encryption-key-id", bit_array.from_string(key.id)),
    ]),
    data: aes_gcm_encrypt(secret.reveal(key.secret), codec.to_bytes(payload)),
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
      aes_gcm_decrypt(secret.reveal(key.secret), payload.data)
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
