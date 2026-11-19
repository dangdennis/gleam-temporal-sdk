//// Failures, as Temporal records them for workflows and activities.

import gleam/option.{type Option, None, Some}
import temporal/codec.{type Payload}

/// What produced a failure.
pub type Kind {
  /// Raised by workflow or activity code.
  Application
  /// A Temporal timeout passed.
  Timeout
  /// Cancellation was requested and accepted.
  Cancelled
  /// The workflow was terminated from outside.
  Terminated
  /// The Temporal service itself failed.
  Server
  /// Wraps the failure of an activity; the real error is in `cause`.
  Activity
  /// Wraps the failure of a child workflow; the real error is in `cause`.
  ChildWorkflow
  /// Wraps the failure of a Nexus operation; the real error is in `cause`.
  NexusOperation
  /// A Nexus handler could not process a request. `error_type` is the Nexus error type.
  NexusHandler
  /// A kind this SDK does not know.
  Other
}

/// An error as Temporal records it. Workflow and activity functions return one to fail,
/// and receive one when something they waited on failed.
pub type Failure {
  Failure(
    message: String,
    kind: Kind,
    /// Application-defined category, matched by retry policies' non-retryable types.
    error_type: String,
    /// Retry policies give up on the failure instead of retrying.
    non_retryable: Bool,
    /// The failure this one wraps.
    cause: Option(Failure),
    /// Extra data attached by the code that raised an application failure.
    details: List(Payload),
  )
}

/// A retryable application failure.
pub fn new(message: String) -> Failure {
  Failure(message, Application, "", False, None, [])
}

/// Set the failure's category. Retry policies can name categories that should not be
/// retried, and callers match on it to tell failures apart.
pub fn with_type(failure: Failure, error_type: String) -> Failure {
  Failure(..failure, error_type:)
}

/// Mark the failure so that retry policies do not retry it.
pub fn non_retryable(failure: Failure) -> Failure {
  Failure(..failure, non_retryable: True)
}

/// What a workflow returns to acknowledge a cancellation request.
pub fn cancelled() -> Failure {
  Failure("cancelled", Cancelled, "", False, None, [])
}

/// The innermost failure: the original error beneath activity and child workflow wrappers.
pub fn root_cause(failure: Failure) -> Failure {
  case failure.cause {
    Some(cause) -> root_cause(cause)
    None -> failure
  }
}

/// Attach data to the failure for whoever handles it.
pub fn with_details(failure: Failure, details: List(Payload)) -> Failure {
  Failure(..failure, details:)
}

/// A failure of kind `Timeout`.
pub fn timeout(message: String) -> Failure {
  Failure(message, Timeout, "", False, None, [])
}

/// Whether the failure is a cancellation, directly or as the cause of an activity or child
/// workflow failure.
pub fn is_cancellation(failure: Failure) -> Bool {
  case failure.kind, failure.cause {
    Cancelled, _ -> True
    Activity, Some(cause)
    | ChildWorkflow, Some(cause)
    | NexusOperation, Some(cause)
    -> is_cancellation(cause)
    _, _ -> False
  }
}

/// Whether the failure is a timeout, directly or as the cause of an activity or child
/// workflow failure.
pub fn is_timeout(failure: Failure) -> Bool {
  case failure.kind, failure.cause {
    Timeout, _ -> True
    Activity, Some(cause)
    | ChildWorkflow, Some(cause)
    | NexusOperation, Some(cause)
    -> is_timeout(cause)
    _, _ -> False
  }
}
