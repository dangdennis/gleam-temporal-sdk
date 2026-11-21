//// What a workflow's failure looks like to everyone else: on the server and to the other
//// SDKs' clients. A workflow that returns a failure it received must pass it on as it
//// was, an activity's failure as an activity failure with its cause, and not as a plain
//// application failure that only shares the message.

import gleam/bit_array
import gleam/dict
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import gleam/time/duration
import support.{type Env, cmd, start_options}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/encryption
import temporal/failure.{type Failure}
import temporal/replayer
import temporal/retry
import temporal/worker
import temporal/workflow.{type Context}

/// An activity that timed out, as the server reports it, with the fields
/// `failure.Failure` does not model.
@external(erlang, "temporal_nif", "sample_activity_failure")
fn sample_activity_failure() -> Failure

fn decline() {
  activity.Definition("decline", codec.nil(), codec.nil())
}

fn stall() {
  activity.Definition("stall", codec.nil(), codec.nil())
}

/// Returns a failure that is not its own: one it received from somewhere else.
fn relay() {
  activity.Definition("relay", codec.nil(), codec.nil())
}

fn activities() -> List(activity.Handler) {
  [
    activity.handler(decline(), fn(_, _) {
      Error(
        failure.new("card declined")
        |> failure.with_type("Declined")
        |> failure.with_details([codec.encode(codec.string(), "insufficient")])
        |> failure.non_retryable,
      )
    }),
    activity.handler(stall(), fn(_, _) {
      process.sleep(3000)
      Ok(Nil)
    }),
    activity.handler(relay(), fn(_, _) { Error(sample_activity_failure()) }),
  ]
}

fn once() -> workflow.ActivityOptions {
  workflow.activity_options(start_to_close: duration.milliseconds(500))
  |> workflow.with_retry(retry.policy(maximum_attempts: 1))
}

/// Runs the activity it is named after and returns its failure as it came.
fn passing_on(name: String, definition: activity.Definition(Nil, Nil)) {
  workflow.handler(
    workflow.Definition(name, codec.nil(), codec.nil()),
    fn(ctx: Context, _: Nil) {
      workflow.execute_activity(ctx, definition, Nil, once())
    },
  )
}

fn relabelling() {
  workflow.Definition("relabelling", codec.nil(), codec.nil())
}

/// Returns the activity's failure with a type of its own.
fn relabelling_workflow(ctx: Context, _: Nil) -> Result(Nil, Failure) {
  workflow.execute_activity(ctx, decline(), Nil, once())
  |> result.map_error(failure.with_type(_, "OrderAbandoned"))
}

fn workflows() -> List(workflow.Handler) {
  [
    passing_on("passing_on_decline", decline()),
    passing_on("passing_on_stall", stall()),
    passing_on("passing_on_relay", relay()),
    workflow.handler(relabelling(), relabelling_workflow),
  ]
}

fn in_both_modes(name: String, scenario: fn(Env) -> Nil) -> Nil {
  use config <- support.in_both_modes(name, support.local_client(), _, scenario)
  config
  |> worker.with_workflows(workflows())
  |> worker.with_activities(activities())
}

/// One failure of a cause chain as the server stores it: which `...FailureInfo` it has,
/// its message, and that info printed, to look for a field's value in.
type Link {
  Link(info: String, message: String, fields: String)
}

fn chain(failure: Dynamic) -> List(Link) {
  let assert Ok(fields) =
    decode.run(failure, decode.dict(decode.string, decode.dynamic))
  let assert Ok(#(info, info_fields)) =
    dict.to_list(fields)
    |> list.find(fn(field) { string.ends_with(field.0, "FailureInfo") })
  let message =
    dict.get(fields, "message")
    |> result.try(fn(message) {
      decode.run(message, decode.string) |> result.replace_error(Nil)
    })
    |> result.unwrap("")
  let rest = case dict.get(fields, "cause") {
    Ok(cause) -> chain(cause)
    Error(Nil) -> []
  }
  [Link(info, message, string.inspect(info_fields)), ..rest]
}

/// Run the workflow, which fails, and return the failure it closed with: as this SDK's
/// client reports it, and as the server recorded it, read with the Temporal CLI.
fn close_failure(env: Env, workflow_type: String) -> #(Failure, List(Link)) {
  let options = start_options(env)
  let assert Ok(handle) =
    client.start(
      env.client,
      workflow.Definition(workflow_type, codec.nil(), codec.nil()),
      Nil,
      options,
    )
  let assert Error(client.WorkflowFailed(seen)) = client.result(handle)
  let assert Ok(history) = client.history(handle)
  assert replayer.replay(workflows(), history) == Ok(Nil)
  let shown =
    cmd(
      "temporal workflow show --output json --workflow-id "
      <> options.workflow_id,
    )
  let closed =
    decode.at(
      ["workflowExecutionFailedEventAttributes", "failure"],
      decode.dynamic,
    )
  let assert Ok(events) =
    json.parse(shown, decode.at(["events"], decode.list(decode.dynamic)))
  let assert Ok(recorded) =
    list.find_map(events, fn(event) {
      decode.run(event, closed) |> result.replace_error(Nil)
    })
  #(seen, chain(recorded))
}

pub fn an_activity_failure_passed_on_stays_an_activity_failure_test() {
  use env <- in_both_modes("pass-on-failure")
  let #(seen, recorded) = close_failure(env, "passing_on_decline")
  let assert [
    Link("activityFailureInfo", _, activity),
    Link("applicationFailureInfo", "card declined", application),
  ] = recorded
  assert string.contains(activity, "\"decline\"")
  assert string.contains(application, "\"Declined\"")
  assert string.contains(application, "nonRetryable")
  // The detail the activity attached, base64 of `"insufficient"` as JSON.
  let detail = bit_array.base64_encode(<<"\"insufficient\"":utf8>>, True)
  assert string.contains(application, detail)

  let assert failure.Failure(
    kind: failure.Activity,
    cause: Some(failure.Failure(
      message: "card declined",
      kind: failure.Application,
      error_type: Some("Declined"),
      non_retryable: True,
      cause: None,
      ..,
    )),
    ..,
  ) = seen
  Nil
}

pub fn an_activity_timeout_passed_on_stays_a_timeout_test() {
  use env <- in_both_modes("pass-on-timeout")
  let #(seen, recorded) = close_failure(env, "passing_on_stall")
  let assert [
    Link("activityFailureInfo", _, activity),
    Link("timeoutFailureInfo", _, timeout),
  ] = recorded
  assert string.contains(activity, "\"stall\"")
  assert string.contains(timeout, "TIMEOUT_TYPE_START_TO_CLOSE")
  assert seen.kind == failure.Activity
  assert failure.is_timeout(seen)
}

pub fn a_changed_failure_is_recorded_as_the_workflow_built_it_test() {
  use env <- in_both_modes("relabel-failure")
  let #(seen, recorded) = close_failure(env, "relabelling")
  // The failure the workflow changed is the workflow's own now; the cause beneath it
  // was not touched and is still what the activity raised.
  let assert [
    Link("applicationFailureInfo", _, relabelled),
    Link("applicationFailureInfo", "card declined", original),
  ] = recorded
  assert string.contains(relabelled, "\"OrderAbandoned\"")
  assert string.contains(original, "\"Declined\"")
  assert seen.kind == failure.Application
  assert seen.error_type == Some("OrderAbandoned")
}

pub fn an_activity_may_fail_with_a_failure_it_received_test() {
  use env <- in_both_modes("relay-failure")
  let #(_, recorded) = close_failure(env, "passing_on_relay")
  // The server takes only application failures from an activity, so the one the activity
  // relayed arrives as the cause of one, whole.
  let assert [
    Link("activityFailureInfo", _, relaying),
    Link("applicationFailureInfo", "Activity task timed out", _),
    Link("activityFailureInfo", "Activity task timed out", relayed),
    Link("timeoutFailureInfo", "activity StartToClose timeout", timeout),
  ] = recorded
  assert string.contains(relaying, "\"relay\"")
  assert string.contains(relayed, "\"the-activity-type\"")
  assert string.contains(relayed, "\"the-worker\"")
  assert string.contains(timeout, "TIMEOUT_TYPE_START_TO_CLOSE")
}

/// With a codec the details inside a failure are encrypted once on their way to the
/// server. Passing the failure on must neither encrypt them again nor send them in the
/// clear.
pub fn details_of_a_failure_passed_on_stay_as_they_were_encoded_test() {
  let assert Ok(key) =
    encryption.key(id: "k1", secret: bit_array.concat(list.repeat(<<7>>, 32)))
  let assert Ok(encrypting) =
    client.connect(
      client.local() |> client.with_codec(encryption.codec(key, [])),
    )
  use env <- support.in_both_modes("pass-on-encrypted", encrypting, fn(config) {
    config
    |> worker.with_workflows(workflows())
    |> worker.with_activities(activities())
  })
  let #(seen, recorded) = close_failure(env, "passing_on_decline")
  let assert [_, Link("applicationFailureInfo", "card declined", stored)] =
    recorded
  assert !string.contains(
    stored,
    bit_array.base64_encode(<<"\"insufficient\"":utf8>>, True),
  )
  let assert Some(failure.Failure(details: [detail], ..)) = seen.cause
  assert codec.decode(codec.string(), detail) == Ok("insufficient")
}
