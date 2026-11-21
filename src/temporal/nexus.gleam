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
        // The caller is another team's workflow, possibly in another namespace: it is
        // told what kind of error this was, and the service's own words stay in this
        // worker's log.
        Error(error) -> {
          let #(error_type, message) = case error {
            client.AlreadyExists(_) -> #(
              "BAD_REQUEST",
              "a workflow for this operation already exists and cannot be started again",
            )
            _ -> #(
              "INTERNAL",
              "could not start the workflow for this operation",
            )
          }
          report(ctx, message, error)
          Error(handler_error(error_type, message))
        }
      }
    },
    cancel: fn(ctx, token) {
      use workflow_id <- result.try(workflow_id_from_token(
        token,
        client.namespace(ctx.client),
      ))
      client.cancel(client.handle(ctx.client, definition, workflow_id))
      |> result.map_error(fn(error) {
        let #(error_type, message) = case error {
          client.NotFound(_) -> #("NOT_FOUND", operation_not_found)
          _ -> #("INTERNAL", "could not cancel the workflow")
        }
        report(ctx, message, error)
        handler_error(error_type, message)
      })
    },
  )
}

const operation_not_found = "operation not found"

/// Log what went wrong with a request, in full, where only this worker's operator reads it.
fn report(ctx: Context, answered: String, error: client.Error) -> Nil {
  bridge.log(
    bridge.Error,
    "temporal: nexus operation "
      <> ctx.service
      <> "/"
      <> ctx.operation
      <> ": "
      <> answered
      <> ": "
      <> string.inspect(error),
    [],
    [],
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

/// The workflow a token names, if the token is one of ours for this handler's namespace.
///
/// The token comes from the caller, who can send any. One that is not a workflow token is
/// a `BAD_REQUEST`, as in the TypeScript SDK. One made for another namespace is answered
/// like a token for a workflow that does not exist: the workflow id in it is not looked up
/// here, where a workflow that happens to share the id would be cancelled in its place.
fn workflow_id_from_token(
  token: String,
  namespace: String,
) -> Result(String, Failure) {
  let decoder = {
    use kind <- decode.field("t", decode.int)
    use token_namespace <- decode.field("ns", decode.string)
    use workflow_id <- decode.field("wid", decode.string)
    decode.success(#(kind, token_namespace, workflow_id))
  }
  let parsed =
    bit_array.base64_url_decode(token)
    |> result.try(fn(bytes) {
      json.parse_bits(bytes, decoder) |> result.replace_error(Nil)
    })
  case parsed {
    Ok(#(kind, token_namespace, workflow_id))
      if kind == workflow_token_type && workflow_id != ""
    ->
      case token_namespace == namespace {
        True -> Ok(workflow_id)
        False -> {
          bridge.log(
            bridge.Warning,
            "temporal: refused to cancel a nexus operation whose token is for namespace \""
              <> token_namespace
              <> "\", not \""
              <> namespace
              <> "\"",
            [],
            [],
          )
          Error(handler_error("NOT_FOUND", operation_not_found))
        }
      }
    _ -> Error(handler_error("BAD_REQUEST", "invalid operation token"))
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
    // The caller is told what kind of error it was; the values involved stay here.
    Error(#(summary, stack_trace, detail)) -> {
      bridge.log(
        bridge.Error,
        "temporal: nexus handler crashed: " <> detail,
        [],
        [],
      )
      Error(
        handler_error("INTERNAL", "nexus handler crashed: " <> summary)
        |> failure.with_stack_trace(stack_trace),
      )
    }
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
    let failure =
      transcode.encode_failure(
        codec,
        client.encodes_failure_attributes(client),
        failure,
      )
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
