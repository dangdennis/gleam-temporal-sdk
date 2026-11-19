//// Applies a payload codec to everything crossing the boundary with core: every payload in
//// an activation, completion, activity task or activity result, wherever it is nested.

import gleam/dict.{type Dict}
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

pub fn decode_failure(
  codec: Option(PayloadCodec),
  failure: Failure,
) -> Result(Failure, String) {
  use details <- result.try(decode_list(codec, failure.details))
  use cause <- result.map(case failure.cause {
    None -> Ok(None)
    Some(cause) -> result.map(decode_failure(codec, cause), Some)
  })
  Failure(..failure, details:, cause:)
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

pub fn decode_activation(
  codec: Option(PayloadCodec),
  activation: bridge.Activation,
) -> Result(bridge.Activation, String) {
  case codec {
    None -> Ok(activation)
    Some(_) ->
      list.try_map(activation.jobs, decode_job(codec, _))
      |> result.map(fn(jobs) { bridge.Activation(..activation, jobs:) })
  }
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

pub fn encode_failure(
  codec: Option(PayloadCodec),
  failure: Failure,
) -> Failure {
  Failure(
    ..failure,
    details: encode_list(codec, failure.details),
    cause: option.map(failure.cause, encode_failure(codec, _)),
  )
}

fn encode_command(
  codec: Option(PayloadCodec),
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
      bridge.RespondToQuery(query_id, Error(encode_failure(codec, failure)))
    bridge.CompleteWorkflow(payload) ->
      bridge.CompleteWorkflow(option.map(payload, encode_one(codec, _)))
    bridge.FailWorkflow(failure) ->
      bridge.FailWorkflow(encode_failure(codec, failure))
    bridge.ContinueAsNew(workflow_type, task_queue, arguments) ->
      bridge.ContinueAsNew(
        workflow_type,
        task_queue,
        encode_list(codec, arguments),
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
      bridge.UpdateRejected(id, encode_failure(codec, failure))
    bridge.ScheduleNexusOperation(input:, ..) ->
      bridge.ScheduleNexusOperation(..command, input: encode_one(codec, input))
    _ -> command
  }
}

pub fn encode_completion(
  codec: Option(PayloadCodec),
  completion: bridge.Completion,
) -> bridge.Completion {
  case codec, completion {
    Some(_), bridge.Success(commands:, ..) ->
      bridge.Success(
        ..completion,
        commands: list.map(commands, encode_command(codec, _)),
      )
    _, _ -> completion
  }
}

pub fn encode_activity_result(
  codec: Option(PayloadCodec),
  result: bridge.ActivityResult,
) -> bridge.ActivityResult {
  case result {
    bridge.ActivityCompleted(payload) ->
      bridge.ActivityCompleted(option.map(payload, encode_one(codec, _)))
    bridge.ActivityFailed(failure) ->
      bridge.ActivityFailed(encode_failure(codec, failure))
    bridge.ActivityCancelled(failure) ->
      bridge.ActivityCancelled(encode_failure(codec, failure))
    bridge.ActivityWillCompleteAsync -> result
  }
}
