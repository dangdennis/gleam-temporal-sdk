//// Failures, as Temporal records them for workflows and activities.

import gleam/option.{type Option, None, Some}

pub type Kind {
  /// Raised by workflow or activity code.
  Application
  Timeout
  Cancelled
  Terminated
  Server
  /// Wraps the failure of an activity; the real error is in `cause`.
  Activity
  /// Wraps the failure of a child workflow; the real error is in `cause`.
  ChildWorkflow
  Other
}

pub type Failure {
  Failure(
    message: String,
    kind: Kind,
    /// Application-defined category, matched by retry policies' non-retryable types.
    error_type: String,
    non_retryable: Bool,
    cause: Option(Failure),
  )
}

/// A retryable application failure.
pub fn new(message: String) -> Failure {
  Failure(message, Application, "", False, None)
}

pub fn with_type(failure: Failure, error_type: String) -> Failure {
  Failure(..failure, error_type:)
}

/// Mark the failure so that retry policies do not retry it.
pub fn non_retryable(failure: Failure) -> Failure {
  Failure(..failure, non_retryable: True)
}

/// What a workflow returns to acknowledge a cancellation request.
pub fn cancelled() -> Failure {
  Failure("cancelled", Cancelled, "", False, None)
}

/// The innermost failure: the original error beneath activity and child workflow wrappers.
pub fn root_cause(failure: Failure) -> Failure {
  case failure.cause {
    Some(cause) -> root_cause(cause)
    None -> failure
  }
}
