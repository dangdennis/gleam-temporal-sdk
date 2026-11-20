//// Implement Nexus operations: entry points one team exposes for workflows in other
//// namespaces to call, by service and operation name, through an endpoint registered on
//// the server.
////
//// A worker serves operations with `worker.with_nexus_handlers`. Workflows call them
//// through `temporal/workflow/operation`. Both sides share the `workflow.NexusOperation`
//// value that names the operation and fixes its types.
////
//// ```gleam
//// pub fn place_order() {
////   workflow.NexusOperation("orders", "place", order_codec(), receipt_codec())
//// }
////
//// // Served by starting a workflow: the operation finishes when the workflow does.
//// nexus.workflow_run(place_order(), order_workflow(), fn(ctx, order) {
////   #(order, client.start_options(workflow_id: "order-" <> ctx.request_id, task_queue: "orders"))
//// })
//// ```

import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/json
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import temporal/client.{type Client}
import temporal/codec.{type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/transcode
import temporal/workflow

/// What a handler knows about the request it is serving.
pub type Context {
  Context(
    /// A client for the namespace the handler's worker runs in.
    client: Client,
    service: String,
    operation: String,
    /// Stable across the caller's retries of the same request: use it to make starting
    /// work idempotent.
    request_id: String,
    headers: Dict(String, String),
  )
}

/// How an operation began: finished already, or running on under a token that names it.
pub type Started {
  Finished(result: Payload)
  Running(operation_token: String)
}

/// An operation implementation, ready to register on a worker.
pub type Handler {
  Handler(
    service: String,
    operation: String,
    start: fn(Request) -> Result(Started, Failure),
    /// Cancel the operation named by a token that `start` returned.
    cancel: fn(Context, String) -> Result(Nil, Failure),
  )
}

/// A start request as the worker received it.
pub type Request {
  Request(
    context: Context,
    input: List(Payload),
    /// Where the server wants an asynchronous operation's result reported.
    callback_url: String,
    callback_headers: Dict(String, String),
  )
}

/// A failure that tells the caller the handler could not process the request, as opposed
/// to the operation itself failing. `error_type` is one of the Nexus error types:
/// `BAD_REQUEST`, `UNAUTHENTICATED`, `UNAUTHORIZED`, `NOT_FOUND` and `NOT_IMPLEMENTED` are
/// final; `RESOURCE_EXHAUSTED`, `INTERNAL`, `UNAVAILABLE` and `UPSTREAM_TIMEOUT` make the
/// caller's server retry.
pub fn handler_error(error_type: String, message: String) -> Failure {
  failure.Failure(
    ..failure.new(message),
    kind: failure.NexusHandler,
    error_type: option.Some(error_type),
  )
}

fn decode_input(
  operation: workflow.NexusOperation(input, output),
  payloads: List(Payload),
) -> Result(input, Failure) {
  codec.decode_arguments(operation.input, payloads)
  |> result.map_error(fn(reason) {
    handler_error("BAD_REQUEST", "could not decode operation input: " <> reason)
  })
}

/// An operation answered directly by a function. For short work only: the caller's server
/// waits about ten seconds for the answer. Return an ordinary failure to fail the
/// operation, or a `handler_error` to report that the request could not be handled.
pub fn sync(
  operation: workflow.NexusOperation(input, output),
  run: fn(Context, input) -> Result(output, Failure),
) -> Handler {
  Handler(
    service: operation.service,
    operation: operation.name,
    start: fn(request) {
      use input <- result.try(decode_input(operation, request.input))
      use output <- result.map(run(request.context, input))
      Finished(codec.encode(operation.output, output))
    },
    cancel: fn(_, _) { Ok(Nil) },
  )
}

/// An operation served by a workflow: it starts the workflow `plan` describes and finishes
/// with that workflow's result. Cancelling the operation cancels the workflow.
///
/// `plan` receives the operation's input and returns the workflow's input and start
/// options. Derive the workflow id from the input or from `ctx.request_id`, so that a
/// retried request reaches the workflow it already started.
pub fn workflow_run(
  operation: workflow.NexusOperation(input, output),
  definition: workflow.Definition(workflow_input, output),
  plan: fn(Context, input) -> #(workflow_input, client.StartOptions),
) -> Handler {
  Handler(
    service: operation.service,
    operation: operation.name,
    start: fn(request) {
      let ctx = request.context
      use input <- result.try(decode_input(operation, request.input))
      let #(workflow_input, options) = plan(ctx, input)
      let callbacks = case request.callback_url {
        "" -> []
        url -> [#(url, request.callback_headers)]
      }
      // A retry of this request must not fail because the first try already started the
      // workflow, so an already-running workflow with this id is taken to be ours.
      let options = client.with_id_conflict_policy(options, client.UseExisting)
      case
        client.start_for_nexus(
          ctx.client,
          definition,
          workflow_input,
          options,
          ctx.request_id,
          callbacks,
        )
      {
        Ok(handle) ->
          Ok(
            Running(workflow_token(
              client.namespace(ctx.client),
              client.workflow_id(handle),
            )),
          )
        Error(client.AlreadyExists(message)) ->
          Error(handler_error("BAD_REQUEST", message))
        Error(error) -> Error(handler_error("INTERNAL", string.inspect(error)))
      }
    },
    cancel: fn(ctx, token) {
      use workflow_id <- result.try(
        workflow_id_from_token(token)
        |> result.replace_error(handler_error(
          "BAD_REQUEST",
          "not a workflow operation token",
        )),
      )
      client.cancel(client.handle(ctx.client, definition, workflow_id))
      |> result.map_error(fn(error) {
        case error {
          client.NotFound(message) -> handler_error("NOT_FOUND", message)
          _ -> handler_error("INTERNAL", "could not cancel the workflow")
        }
      })
    },
  )
}

// The token format the other Temporal SDKs use for workflow-backed operations: unpadded
// URL-safe base64 of a JSON object naming the workflow.
const workflow_token_type = 1

fn workflow_token(namespace: String, workflow_id: String) -> String {
  json.object([
    #("t", json.int(workflow_token_type)),
    #("ns", json.string(namespace)),
    #("wid", json.string(workflow_id)),
  ])
  |> json.to_string
  |> bit_array.from_string
  |> bit_array.base64_url_encode(False)
}

fn workflow_id_from_token(token: String) -> Result(String, Nil) {
  use bytes <- result.try(bit_array.base64_url_decode(token))
  let decoder = {
    use kind <- decode.field("t", decode.int)
    use workflow_id <- decode.field("wid", decode.string)
    decode.success(#(kind, workflow_id))
  }
  case json.parse_bits(bytes, decoder) {
    Ok(#(kind, workflow_id)) if kind == workflow_token_type -> Ok(workflow_id)
    _ -> Error(Nil)
  }
}

fn find_handler(
  registry: Dict(#(String, String), Handler),
  service: String,
  operation: String,
  found: fn(Handler) -> Result(a, Failure),
) -> Result(a, Failure) {
  case dict.get(registry, #(service, operation)) {
    Ok(handler) -> found(handler)
    Error(Nil) ->
      Error(handler_error(
        "NOT_FOUND",
        "no handler for operation \"" <> service <> "/" <> operation <> "\"",
      ))
  }
}

fn guard(body: fn() -> Result(a, Failure)) -> Result(a, Failure) {
  case bridge.rescue_all(body) {
    Ok(outcome) -> outcome
    Error(crash) ->
      Error(handler_error("INTERNAL", "nexus handler crashed: " <> crash))
  }
}

/// Turn a task into its answer. Never crashes: a crash in a handler is an internal error.
@internal
pub fn dispatcher(
  handlers: List(Handler),
  client: Client,
) -> fn(bridge.NexusTask) -> bridge.NexusResult {
  let registry =
    dict.from_list(
      list.map(handlers, fn(handler) {
        #(#(handler.service, handler.operation), handler)
      }),
    )
  let codec = client.payload_codec(client)
  let failed = fn(failure: Failure) {
    let failure = transcode.encode_failure(codec, failure)
    case failure.kind {
      failure.NexusHandler ->
        bridge.NexusHandlerFailed(
          option.unwrap(failure.error_type, "INTERNAL"),
          failure,
        )
      _ -> bridge.NexusOperationFailed(failure)
    }
  }
  fn(task) {
    case task {
      bridge.NexusTaskCancelled(_) -> bridge.NexusTaskCancelAcknowledged
      bridge.NexusStartTask(
        service:,
        operation:,
        request_id:,
        input:,
        callback_url:,
        callback_headers:,
        headers:,
        ..,
      ) -> {
        let outcome =
          guard(fn() {
            use handler <- find_handler(registry, service, operation)
            use input <- result.try(
              transcode.decode_list(codec, option.values([input]))
              |> result.map_error(handler_error("BAD_REQUEST", _)),
            )
            handler.start(Request(
              context: Context(
                client:,
                service:,
                operation:,
                request_id:,
                headers:,
              ),
              input:,
              callback_url:,
              callback_headers:,
            ))
          })
        case outcome {
          Ok(Finished(payload)) ->
            bridge.NexusSyncSuccess(transcode.encode_one(codec, payload))
          Ok(Running(token)) -> bridge.NexusAsyncSuccess(token)
          Error(failure) -> failed(failure)
        }
      }
      bridge.NexusCancelTask(service:, operation:, operation_token:, ..) -> {
        let outcome =
          guard(fn() {
            use handler <- find_handler(registry, service, operation)
            handler.cancel(
              Context(
                client:,
                service:,
                operation:,
                request_id: "",
                headers: dict.new(),
              ),
              operation_token,
            )
          })
        case outcome {
          Ok(Nil) -> bridge.NexusCancelAcknowledged
          Error(failure) -> failed(failure)
        }
      }
    }
  }
}
