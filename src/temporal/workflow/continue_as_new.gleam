//// Continue as new with options: end the run and start the next one on another task queue,
//// with timeouts, or with a different memo, search attributes, retry policy or headers.
//// `workflow.continue_as_new` is the form without options.
////
//// ```gleam
//// let options =
////   continue_as_new.options()
////   |> continue_as_new.with_task_queue("orders-v2")
////   |> continue_as_new.with_run_timeout(duration.hours(1))
//// continue_as_new.run(ctx, poll(), next_cursor, options)
//// ```
////
//// Whatever is not set is kept from the run that ends: its task queue, memo, search
//// attributes, retry policy and headers. The timeouts are the exception: the new run gets
//// the ones given here, and the server's defaults otherwise.

import gleam/dict.{type Dict}
import gleam/option.{type Option, None, Some}
import gleam/time/duration.{type Duration}
import temporal/codec.{type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/time
import temporal/retry
import temporal/search_attributes
import temporal/workflow.{type Context, type Definition}

/// How the next run is started. Build with `options` and the `with_` functions.
pub type Options {
  Options(
    /// `None` keeps the task queue of the run that ends.
    task_queue: Option(String),
    /// Longest the new run may take. `None` leaves it to the server's default.
    run_timeout: Option(Duration),
    /// Longest one workflow task of the new run may take. `None` is the server's default.
    task_timeout: Option(Duration),
    /// `None` keeps the memo of the run that ends.
    memo: Option(Dict(String, Payload)),
    /// `None` keeps the search attributes of the run that ends.
    search_attributes: Option(Dict(String, search_attributes.Value)),
    /// `None` passes on the headers of the run that ends.
    headers: Option(Dict(String, Payload)),
    /// `None` keeps the retry policy of the run that ends.
    retry: Option(retry.Policy),
  )
}

/// Options that change nothing: the new run is started as `workflow.continue_as_new`
/// starts it.
pub fn options() -> Options {
  Options(
    task_queue: None,
    run_timeout: None,
    task_timeout: None,
    memo: None,
    search_attributes: None,
    headers: None,
    retry: None,
  )
}

/// Start the new run on this task queue instead of the current one.
pub fn with_task_queue(options: Options, task_queue: String) -> Options {
  Options(..options, task_queue: Some(task_queue))
}

/// The longest the new run may take.
pub fn with_run_timeout(options: Options, timeout: Duration) -> Options {
  Options(..options, run_timeout: Some(timeout))
}

/// The longest one workflow task of the new run may take.
pub fn with_task_timeout(options: Options, timeout: Duration) -> Options {
  Options(..options, task_timeout: Some(timeout))
}

/// Give the new run this memo instead of the current one. An empty memo cannot replace
/// one: the server reads it as "keep".
pub fn with_memo(options: Options, memo: Dict(String, Payload)) -> Options {
  Options(..options, memo: Some(memo))
}

/// Give the new run these search attributes instead of the current ones. An empty set
/// cannot replace them: the server reads it as "keep".
pub fn with_search_attributes(
  options: Options,
  attributes: Dict(String, search_attributes.Value),
) -> Options {
  Options(..options, search_attributes: Some(attributes))
}

/// Give the new run these headers instead of passing on the current ones. An empty
/// dictionary starts it with none.
pub fn with_headers(
  options: Options,
  headers: Dict(String, Payload),
) -> Options {
  Options(..options, headers: Some(headers))
}

/// Retry the new run by this policy instead of the current one.
pub fn with_retry(options: Options, policy: retry.Policy) -> Options {
  Options(..options, retry: Some(policy))
}

/// End this run and start a fresh one with a new input and an empty history, as the
/// options say. Does not return.
pub fn run(
  ctx: Context,
  definition: Definition(input, output),
  input: input,
  options: Options,
) -> Result(output, Failure) {
  workflow.continue_as_new_with(
    definition,
    input,
    option.unwrap(options.task_queue, workflow.info(ctx).task_queue),
    bridge.ContinueAsNewOptions(
      run_timeout_ms: time.option_to_ms(options.run_timeout),
      task_timeout_ms: time.option_to_ms(options.task_timeout),
      memo: option.unwrap(options.memo, dict.new()),
      search_attributes: case options.search_attributes {
        Some(attributes) -> search_attributes.encode(attributes)
        None -> dict.new()
      },
      headers: option.unwrap(options.headers, workflow.headers(ctx)),
      retry: option.map(options.retry, retry.to_bridge),
    ),
  )
}
