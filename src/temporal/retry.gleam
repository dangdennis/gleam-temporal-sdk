//// Retry policies: how often and how fast a failed activity, child workflow or workflow
//// is tried again. One type serves every place that retries.
////
//// ```gleam
//// let policy =
////   retry.policy(maximum_attempts: 5)
////   |> retry.with_initial_interval(duration.milliseconds(200))
////   |> retry.with_non_retryable_error_types(["CardDeclined"])
//// let options =
////   workflow.activity_options(start_to_close: duration.seconds(10))
////   |> workflow.with_retry(policy)
//// ```

import gleam/option.{type Option, None, Some}
import gleam/time/duration.{type Duration}
import temporal/internal/bridge
import temporal/internal/time

/// How failures are retried. The wait before attempt `n` is `initial_interval` times
/// `backoff_coefficient` to the power `n - 1`, capped at `maximum_interval`. Build with
/// `policy` and the `with_` functions.
pub type Policy {
  Policy(
    /// The wait before the second attempt.
    initial_interval: Duration,
    /// What the wait is multiplied by after each attempt.
    backoff_coefficient: Float,
    /// The longest wait. `None` is one hundred times the initial interval.
    maximum_interval: Option(Duration),
    /// Zero means unlimited.
    maximum_attempts: Int,
    /// Failures whose `error_type` is listed here are not retried.
    non_retryable_error_types: List(String),
  )
}

/// A policy that gives up after `maximum_attempts` attempts, one second apart at first and
/// doubling. Zero attempts means no limit.
pub fn policy(maximum_attempts maximum_attempts: Int) -> Policy {
  Policy(
    initial_interval: duration.seconds(1),
    backoff_coefficient: 2.0,
    maximum_interval: None,
    maximum_attempts:,
    non_retryable_error_types: [],
  )
}

/// The wait before the second attempt.
pub fn with_initial_interval(policy: Policy, interval: Duration) -> Policy {
  Policy(..policy, initial_interval: interval)
}

/// What the wait is multiplied by after each attempt. `1.0` retries at a fixed interval.
pub fn with_backoff_coefficient(policy: Policy, coefficient: Float) -> Policy {
  Policy(..policy, backoff_coefficient: coefficient)
}

/// The longest wait between attempts. Unset, it is one hundred times the initial interval.
pub fn with_maximum_interval(policy: Policy, interval: Duration) -> Policy {
  Policy(..policy, maximum_interval: Some(interval))
}

/// Do not retry failures whose `error_type` is one of these.
pub fn with_non_retryable_error_types(
  policy: Policy,
  error_types: List(String),
) -> Policy {
  Policy(..policy, non_retryable_error_types: error_types)
}

@internal
pub fn to_bridge(policy: Policy) -> bridge.RetryPolicy {
  bridge.RetryPolicy(
    initial_interval_ms: time.to_ms(policy.initial_interval),
    backoff_coefficient: policy.backoff_coefficient,
    maximum_interval_ms: time.option_to_ms(policy.maximum_interval),
    maximum_attempts: policy.maximum_attempts,
    non_retryable_error_types: policy.non_retryable_error_types,
  )
}

@internal
pub fn from_bridge(policy: bridge.RetryPolicy) -> Policy {
  Policy(
    initial_interval: time.from_ms(policy.initial_interval_ms),
    backoff_coefficient: policy.backoff_coefficient,
    maximum_interval: time.option_from_ms(policy.maximum_interval_ms),
    maximum_attempts: policy.maximum_attempts,
    non_retryable_error_types: policy.non_retryable_error_types,
  )
}
