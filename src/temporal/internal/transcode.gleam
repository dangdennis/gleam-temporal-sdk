//// Applies a payload codec to everything crossing the boundary with core: every payload in
//// an activation, completion, activity task or activity result, wherever it is nested.
////
//// A failure's message and stack trace are text, not payloads, and so are outside the
//// codec unless they are moved into the failure's `encoded_attributes` payload, as the
//// option `client.with_encoded_failure_attributes` has done here and
//// `encodeCommonAttributes` in Temporal's other SDKs. That payload is a JSON object,
//// `{"message": ..., "stack_trace": ...}`, sent through the codec like any other.
//// Headers are not transcoded: see `temporal/encryption`.

import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import temporal/codec.{type Payload, type PayloadCodec}
import temporal/failure.{type Failure, Failure}
import temporal/internal/bridge

// ---- decoding: from the service ----

pub fn decode_list(
  codec: Option(PayloadCodec),
  payloads: List(Payload),
) -> Result(List(Payload), String) {
  case codec, payloads {
    None, _ | _, [] -> Ok(payloads)
    Some(codec), _ -> codec.decode(payloads)
  }
}

fn decode_one(
  codec: Option(PayloadCodec),
  payload: Payload,
) -> Result(Payload, String) {
  use decoded <- result.try(decode_list(codec, [payload]))
  case decoded {
    [payload] -> Ok(payload)
    _ -> Error("payload codec did not return one payload for one payload")
  }
}

fn decode_option(
  codec: Option(PayloadCodec),
  payload: Option(Payload),
) -> Result(Option(Payload), String) {
  case payload {
    None -> Ok(None)
    Some(payload) -> result.map(decode_one(codec, payload), Some)
  }
}

pub fn decode_map(
  codec: Option(PayloadCodec),
  payloads: Dict(String, Payload),
) -> Result(Dict(String, Payload), String) {
  dict.to_list(payloads)
  |> list.try_map(fn(entry) {
    result.map(decode_one(codec, entry.1), fn(payload) { #(entry.0, payload) })
  })
  |> result.map(dict.from_list)
}

/// Decode a failure's details and, if its message and stack trace arrived encoded,
/// restore them, whichever SDK encoded them. `encoded_attributes` stays as it arrived:
/// `encode_failure` tells by it that the failure came encoded.
///
/// The failure's `original` is left alone, here and in `encode_failure`: the details inside
/// it stay as the service sent them, which is how they go back if the failure is returned.
pub fn decode_failure(
  codec: Option(PayloadCodec),
  failure: Failure,
) -> Result(Failure, String) {
  use details <- result.try(decode_list(codec, failure.details))
  use cause <- result.try(case failure.cause {
    None -> Ok(None)
    Some(cause) -> result.map(decode_failure(codec, cause), Some)
  })
  use #(message, stack_trace) <- result.map(case failure.encoded_attributes {
    None -> Ok(#(failure.message, failure.stack_trace))
    Some(encoded) ->
      result.map(decode_one(codec, encoded), restored(_, failure))
  })
  Failure(..failure, message:, stack_trace:, details:, cause:)
}

/// The message and stack trace in a failure's decoded attributes. Whatever is missing or
/// is not text is left as the failure has it, as the TypeScript SDK does.
fn restored(attributes: Payload, failure: Failure) -> #(String, String) {
  let text = fn(name, otherwise) {
    decode.optional_field(
      name,
      otherwise,
      decode.one_of(decode.string, [decode.success(otherwise)]),
      decode.success,
    )
  }
  let decoder = {
    use message <- decode.then(text("message", failure.message))
    use stack_trace <- decode.then(text("stack_trace", failure.stack_trace))
    decode.success(#(message, stack_trace))
  }
  case dict.get(attributes.metadata, "encoding") {
    Ok(<<"json/plain":utf8>>) ->
      json.parse_bits(attributes.data, decoder)
      |> result.unwrap(#(failure.message, failure.stack_trace))
    _ -> #(failure.message, failure.stack_trace)
  }
}

fn decode_resolution(
  codec: Option(PayloadCodec),
  resolution: bridge.Resolution,
) -> Result(bridge.Resolution, String) {
  case resolution {
    bridge.Completed(payload) ->
      result.map(decode_option(codec, payload), bridge.Completed)
    bridge.Failed(failure) ->
      result.map(decode_failure(codec, failure), bridge.Failed)
    bridge.Cancelled(failure) ->
      result.map(decode_failure(codec, failure), bridge.Cancelled)
    bridge.Backoff(..) -> Ok(resolution)
  }
}

fn decode_job(
  codec: Option(PayloadCodec),
  job: bridge.Job,
) -> Result(bridge.Job, String) {
  case job {
    bridge.InitializeWorkflow(arguments:, memo:, ..) -> {
      use arguments <- result.try(decode_list(codec, arguments))
      use memo <- result.map(decode_map(codec, memo))
      bridge.InitializeWorkflow(..job, arguments:, memo:)
    }
    bridge.ResolveActivity(seq, resolution) ->
      result.map(decode_resolution(codec, resolution), bridge.ResolveActivity(
        seq,
        _,
      ))
    bridge.ResolveChildWorkflow(seq, resolution) ->
      result.map(
        decode_resolution(codec, resolution),
        bridge.ResolveChildWorkflow(seq, _),
      )
    bridge.ResolveChildWorkflowStart(seq, bridge.ChildStartCancelled(failure)) ->
      result.map(decode_failure(codec, failure), fn(failure) {
        bridge.ResolveChildWorkflowStart(
          seq,
          bridge.ChildStartCancelled(failure),
        )
      })
    bridge.QueryWorkflow(query_id, query_type, arguments) ->
      result.map(decode_list(codec, arguments), bridge.QueryWorkflow(
        query_id,
        query_type,
        _,
      ))
    bridge.SignalWorkflow(name, input) ->
      result.map(decode_list(codec, input), bridge.SignalWorkflow(name, _))
    bridge.DoUpdate(input:, ..) ->
      result.map(decode_list(codec, input), fn(input) {
        bridge.DoUpdate(..job, input:)
      })
    bridge.ResolveExternalSignal(seq, Some(failure)) ->
      result.map(decode_failure(codec, failure), fn(failure) {
        bridge.ResolveExternalSignal(seq, Some(failure))
      })
    bridge.ResolveExternalCancel(seq, Some(failure)) ->
      result.map(decode_failure(codec, failure), fn(failure) {
        bridge.ResolveExternalCancel(seq, Some(failure))
      })
    bridge.ResolveNexusStart(seq, bridge.NexusStartFailed(failure)) ->
      result.map(decode_failure(codec, failure), fn(failure) {
        bridge.ResolveNexusStart(seq, bridge.NexusStartFailed(failure))
      })
    bridge.ResolveNexus(seq, resolution) ->
      result.map(decode_resolution(codec, resolution), bridge.ResolveNexus(
        seq,
        _,
      ))
    _ -> Ok(job)
  }
}

/// Jobs are walked with or without a codec: a failure's attributes can be encoded as plain
/// JSON, by an SDK with the option on and no codec.
pub fn decode_activation(
  codec: Option(PayloadCodec),
  activation: bridge.Activation,
) -> Result(bridge.Activation, String) {
  list.try_map(activation.jobs, decode_job(codec, _))
  |> result.map(fn(jobs) { bridge.Activation(..activation, jobs:) })
}

pub fn decode_activity_task(
  codec: Option(PayloadCodec),
  task: bridge.ActivityTask,
) -> Result(bridge.ActivityTask, String) {
  case task {
    bridge.ActivityStart(input:, heartbeat_details:, ..) -> {
      use input <- result.try(decode_list(codec, input))
      use heartbeat_details <- result.map(decode_list(codec, heartbeat_details))
      bridge.ActivityStart(..task, input:, heartbeat_details:)
    }
    bridge.ActivityCancel(..) -> Ok(task)
  }
}

// ---- encoding: to the service ----

pub fn encode_list(
  codec: Option(PayloadCodec),
  payloads: List(Payload),
) -> List(Payload) {
  case codec, payloads {
    None, _ | _, [] -> payloads
    Some(codec), _ -> codec.encode(payloads)
  }
}

pub fn encode_one(codec: Option(PayloadCodec), payload: Payload) -> Payload {
  case encode_list(codec, [payload]) {
    [encoded] -> encoded
    _ -> payload
  }
}

pub fn encode_map(
  codec: Option(PayloadCodec),
  payloads: Dict(String, Payload),
) -> Dict(String, Payload) {
  case codec {
    None -> payloads
    Some(_) ->
      dict.map_values(payloads, fn(_, payload) { encode_one(codec, payload) })
  }
}

/// What a failure's message reads, in every Temporal SDK, when the real one is in its
/// encoded attributes.
const encoded_failure = "Encoded failure"

/// Encode a failure's details and, with `encode_attributes`, its message and stack trace,
/// and the same for its causes.
///
/// The message and stack trace stay in the failure given to the NIF, which needs them to
/// tell whether a received failure is returned unchanged; it sends `Encoded failure` and
/// no stack trace in their place whenever `encoded_attributes` is set.
///
/// A failure that arrived encoded and still reads as it did keeps the attributes it came
/// with, with or without the option: what arrived hidden is never sent in the clear. One
/// whose message was changed since is encoded afresh, or not, as the option says.
pub fn encode_failure(
  codec: Option(PayloadCodec),
  encode_attributes: Bool,
  failure: Failure,
) -> Failure {
  let fresh = fn() {
    case encode_attributes {
      True ->
        Some(encode_one(codec, attributes(failure.message, failure.stack_trace)))
      False -> None
    }
  }
  let encoded_attributes = case failure.encoded_attributes {
    None -> fresh()
    Some(arrived) ->
      case reads_as_it_arrived(codec, arrived, failure) {
        True -> Some(arrived)
        False -> fresh()
      }
  }
  Failure(
    ..failure,
    details: encode_list(codec, failure.details),
    cause: option.map(failure.cause, encode_failure(codec, encode_attributes, _)),
    encoded_attributes:,
  )
}

/// The payload the TypeScript SDK's `DefaultFailureConverter` and the Go SDK's make of a
/// message and stack trace, before the codec.
fn attributes(message: String, stack_trace: String) -> Payload {
  codec.Payload(
    metadata: dict.from_list([#("encoding", <<"json/plain":utf8>>)]),
    data: json.object([
      #("message", json.string(message)),
      #("stack_trace", json.string(stack_trace)),
    ])
      |> json.to_string
      |> bit_array.from_string,
  )
}

/// Whether a failure that arrived with encoded attributes still has the message and stack
/// trace they hold. Attributes this codec cannot read were never restored.
fn reads_as_it_arrived(
  codec: Option(PayloadCodec),
  arrived: Payload,
  failure: Failure,
) -> Bool {
  case decode_one(codec, arrived) {
    Ok(decoded) ->
      restored(decoded, failure) == #(failure.message, failure.stack_trace)
    Error(_) -> failure.message == encoded_failure
  }
}

fn encode_command(
  codec: Option(PayloadCodec),
  encode_attributes: Bool,
  command: bridge.Command,
) -> bridge.Command {
  case command {
    bridge.ScheduleActivity(arguments:, ..) ->
      bridge.ScheduleActivity(
        ..command,
        arguments: encode_list(codec, arguments),
      )
    bridge.ScheduleLocalActivity(arguments:, ..) ->
      bridge.ScheduleLocalActivity(
        ..command,
        arguments: encode_list(codec, arguments),
      )
    bridge.RespondToQuery(query_id, Ok(payload)) ->
      bridge.RespondToQuery(query_id, Ok(encode_one(codec, payload)))
    bridge.RespondToQuery(query_id, Error(failure)) ->
      bridge.RespondToQuery(
        query_id,
        Error(encode_failure(codec, encode_attributes, failure)),
      )
    bridge.CompleteWorkflow(payload) ->
      bridge.CompleteWorkflow(option.map(payload, encode_one(codec, _)))
    bridge.FailWorkflow(failure) ->
      bridge.FailWorkflow(encode_failure(codec, encode_attributes, failure))
    bridge.ContinueAsNew(arguments:, options:, ..) ->
      bridge.ContinueAsNew(
        ..command,
        arguments: encode_list(codec, arguments),
        options: bridge.ContinueAsNewOptions(
          ..options,
          memo: encode_map(codec, options.memo),
        ),
      )
    bridge.StartChildWorkflow(input:, options:, ..) ->
      bridge.StartChildWorkflow(
        ..command,
        input: encode_list(codec, input),
        options: bridge.ChildStartOptions(
          ..options,
          memo: encode_map(codec, options.memo),
        ),
      )
    bridge.SignalExternal(input:, ..) ->
      bridge.SignalExternal(..command, input: encode_list(codec, input))
    bridge.UpdateCompleted(id, payload) ->
      bridge.UpdateCompleted(id, encode_one(codec, payload))
    bridge.UpdateRejected(id, failure) ->
      bridge.UpdateRejected(
        id,
        encode_failure(codec, encode_attributes, failure),
      )
    bridge.ScheduleNexusOperation(input:, ..) ->
      bridge.ScheduleNexusOperation(..command, input: encode_one(codec, input))
    _ -> command
  }
}

/// A completion is walked with or without a codec, for the failures in it.
pub fn encode_completion(
  codec: Option(PayloadCodec),
  encode_attributes: Bool,
  completion: bridge.Completion,
) -> bridge.Completion {
  case completion {
    bridge.Success(commands:, ..) ->
      bridge.Success(
        ..completion,
        commands: list.map(commands, encode_command(codec, encode_attributes, _)),
      )
    bridge.TaskFailure(run_id, failure) ->
      bridge.TaskFailure(
        run_id,
        encode_failure(codec, encode_attributes, failure),
      )
  }
}

pub fn encode_activity_result(
  codec: Option(PayloadCodec),
  encode_attributes: Bool,
  result: bridge.ActivityResult,
) -> bridge.ActivityResult {
  case result {
    bridge.ActivityCompleted(payload) ->
      bridge.ActivityCompleted(option.map(payload, encode_one(codec, _)))
    bridge.ActivityFailed(failure) ->
      bridge.ActivityFailed(encode_failure(codec, encode_attributes, failure))
    bridge.ActivityCancelled(failure) ->
      bridge.ActivityCancelled(encode_failure(codec, encode_attributes, failure))
    bridge.ActivityWillCompleteAsync -> result
  }
}
