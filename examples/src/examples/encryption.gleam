//// Encryption: a payload codec that keeps plaintext away from the Temporal service, and
//// how to rotate its key.
////
//// A payload codec transforms every payload a client and its workers send or receive, so
//// the service stores only ciphertext. Workflow and activity code does not change. The
//// included codec is AES-256-GCM in the format of the codec in Temporal's own samples.
////
//// Keys are 32 bytes and belong in a secret store, never in source code: `run` takes them
//// as arguments. `encryption.key` makes one, and refuses a secret of any other size.
////
//// A codec covers payloads. A failure's message is text beside them, so the client here
//// also turns on `client.with_encoded_failure_attributes`, which sends failure messages
//// and stack traces through the codec too. What stays readable on the service, with or
//// without it: search attributes, ids, type names, task queues and headers. See
//// `temporal/encryption` for the whole list, and for `encryption.strict`, which refuses
//// unencrypted payloads once nothing older is left to read.

import gleam/result
import temporal/client
import temporal/codec
import temporal/encryption
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow
import temporal/workflow/signal

const task_queue = "examples-encryption"

/// What `run` observed.
pub type Outcome {
  Outcome(
    /// The result, read with the new key from a workflow started with the old one.
    result: String,
    /// Whether a client without the codec could read the result.
    readable_without_key: Bool,
  )
}

pub fn keep_secret() {
  workflow.Definition("keep_secret", codec.string(), codec.string())
}

pub fn reveal() {
  workflow.SignalDefinition("reveal", codec.string())
}

fn keep_secret_workflow(
  ctx: workflow.Context,
  secret: String,
) -> Result(String, Failure) {
  let suffix = signal.receive(ctx, reveal())
  Ok(secret <> suffix)
}

/// Connect with a codec that encrypts with `current` and can still decrypt with any of
/// `retired`, and with failure messages encoded as well.
fn connect(
  options: client.Options,
  current: encryption.Key,
  retired: List(encryption.Key),
) -> Result(client.Client, client.Error) {
  client.connect(
    options
    |> client.with_codec(encryption.codec(current, retired))
    |> client.with_encoded_failure_attributes,
  )
}

fn start_worker(client: client.Client) -> Result(worker.Worker, client.Error) {
  worker.new(client, task_queue)
  |> worker.with_workflows([
    workflow.handler(keep_secret(), keep_secret_workflow),
  ])
  |> worker.start
}

/// `options` says where the service is, for example `client.local()`.
pub fn run(
  options: client.Options,
  old_key: encryption.Key,
  new_key: encryption.Key,
) -> Result(Outcome, client.Error) {
  let workflow_id = "examples-encryption"

  // Before the rotation: everything is written with the old key.
  use before <- result.try(connect(options, old_key, []))
  use worker <- result.try(start_worker(before))
  let started =
    client.start(
      before,
      keep_secret(),
      "the vault code is 1234",
      client.start_options(workflow_id:, task_queue:),
    )
  worker.stop(worker)
  use _ <- result.try(started)

  // After the rotation: new payloads are written with the new key. The old key stays in
  // the retired list for as long as histories written with it may be read.
  use after <- result.try(connect(options, new_key, [old_key]))
  use worker <- result.try(start_worker(after))
  let handle = client.handle(after, keep_secret(), workflow_id)
  let outcome = {
    use _ <- result.try(client.signal(handle, reveal(), "!"))
    use result <- result.try(client.result(handle))

    // Without the codec the result is ciphertext, which does not decode as a string.
    use plain <- result.try(client.connect(options))
    let readable_without_key =
      client.handle(plain, keep_secret(), workflow_id)
      |> client.result
      |> result.is_ok
    Ok(Outcome(result:, readable_without_key:))
  }
  worker.stop(worker)
  outcome
}
