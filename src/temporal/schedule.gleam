//// Schedules: start a workflow on a timetable, without a worker having to be running when
//// it falls due.
////
//// ```gleam
//// let nightly =
////   schedule.new(
////     schedule.every(duration.hours(24)) |> schedule.with_offset(duration.hours(3)),
////     schedule.action(
////       cleanup(),
////       Nil,
////       client.start_options(workflow_id: "cleanup", task_queue: "main"),
////     ),
////   )
////   |> schedule.with_overlap(schedule.BufferOne)
//// let assert Ok(Nil) = schedule.create(client, "nightly-cleanup", nightly)
//// ```
////
//// A timetable is a `Spec`, from `every`, `on` or `cron`. What it starts is an `Action`,
//// from `action`. Each is adjusted with the `with_` functions that take it.

import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import temporal/client.{type Client, type Error}
import temporal/codec.{type Payload}
import temporal/internal/bridge
import temporal/internal/time
import temporal/internal/transcode
import temporal/retry
import temporal/workflow

/// When the schedule fires: the union of its intervals, calendars and cron expressions.
/// Build with `every`, `on` or `cron`, then `with_offset`, `with_timezone` and `with_jitter`.
pub type Spec {
  Spec(
    intervals: List(Interval),
    calendars: List(Calendar),
    /// Cron expressions such as `0 9 * * MON-FRI`. The server turns these into calendars,
    /// so a described schedule reports them under `calendars` instead.
    cron_expressions: List(String),
    /// An IANA time zone name for calendars and cron expressions. `None` is UTC.
    timezone: Option(String),
    /// Delay each action by a random amount up to this, to spread load.
    jitter: Option(Duration),
  )
}

/// Fire once every `every`, at times that are `offset` past a multiple of `every` since
/// the Unix epoch.
pub type Interval {
  Interval(every: Duration, offset: Duration)
}

/// Fire when the time matches every field. An empty `year` matches every year. Build with
/// `daily` and the `with_` functions named after the fields.
pub type Calendar {
  Calendar(
    second: List(Range),
    minute: List(Range),
    hour: List(Range),
    day_of_month: List(Range),
    month: List(Range),
    year: List(Range),
    /// 0 is Sunday.
    day_of_week: List(Range),
    comment: String,
  )
}

/// The values from `start` to `end`, inclusive, `step` apart.
pub type Range {
  Range(start: Int, end: Int, step: Int)
}

/// What to do when an action falls due while the previous one is still running.
pub type OverlapPolicy {
  /// Do not start. The default.
  Skip
  /// Start one more as soon as the running one finishes.
  BufferOne
  /// Start every missed action in turn.
  BufferAll
  CancelOther
  TerminateOther
  /// Start regardless, so several run at once.
  AllowAll
}

/// The workflow a schedule starts. Build with `action`.
pub type Action {
  Action(
    workflow_type: String,
    /// The server appends the scheduled time, so each run has its own id.
    workflow_id: String,
    task_queue: String,
    arguments: List(Payload),
    execution_timeout: Option(Duration),
    run_timeout: Option(Duration),
    task_timeout: Option(Duration),
    retry: Option(retry.Policy),
    memo: Dict(String, Payload),
    search_attributes: Dict(String, Payload),
  )
}

/// A timetable, the workflow it starts, and how it behaves. Build with `new` and the
/// `with_` functions that take a schedule.
pub type Schedule {
  Schedule(
    spec: Spec,
    action: Action,
    /// What to do when an action falls due while the previous one is still running.
    overlap: OverlapPolicy,
    /// How far back to make up for actions missed while the server was down. `None` uses
    /// the server's default of one year.
    catchup_window: Option(Duration),
    /// Pause the schedule when a workflow it started fails.
    pause_on_failure: Bool,
    /// A paused schedule takes no actions.
    paused: Bool,
    /// A human-readable note, typically why the schedule is paused.
    note: String,
  )
}

/// A schedule as the server has it, with what it has done and will do.
pub type Description {
  Description(
    schedule: Schedule,
    /// How many actions the schedule has taken.
    action_count: Int,
    /// The most recent actions, oldest first.
    recent_actions: List(RecentAction),
    /// When the next few actions are due.
    next_action_times: List(Timestamp),
    running_workflows: Int,
  )
}

/// One time the schedule started its workflow.
pub type RecentAction {
  RecentAction(scheduled_time: Timestamp, workflow_id: String)
}

/// A schedule as it appears in a listing.
pub type Summary {
  Summary(
    schedule_id: String,
    workflow_type: String,
    paused: Bool,
    note: String,
  )
}

// ---- building ----

/// A spec that never fires, to build on.
fn empty() -> Spec {
  Spec(
    intervals: [],
    calendars: [],
    cron_expressions: [],
    timezone: None,
    jitter: None,
  )
}

/// A spec that fires once every `interval`, at multiples of it since the Unix epoch.
pub fn every(interval: Duration) -> Spec {
  Spec(..empty(), intervals: [
    Interval(every: interval, offset: duration.seconds(0)),
  ])
}

/// A spec that fires when the calendar matches.
pub fn on(calendar: Calendar) -> Spec {
  Spec(..empty(), calendars: [calendar])
}

/// A spec from a cron expression.
pub fn cron(expression: String) -> Spec {
  Spec(..empty(), cron_expressions: [expression])
}

/// Fire the spec's intervals `offset` after the epoch-aligned times. Give schedules with
/// the same interval different offsets so they do not all fire together. Calendars and
/// cron expressions are not affected.
pub fn with_offset(spec: Spec, offset: Duration) -> Spec {
  Spec(
    ..spec,
    intervals: list.map(spec.intervals, fn(interval) {
      Interval(..interval, offset:)
    }),
  )
}

/// Interpret the spec's calendars and cron expressions in an IANA time zone, such as
/// `Europe/Lisbon`, instead of UTC.
pub fn with_timezone(spec: Spec, timezone: String) -> Spec {
  Spec(..spec, timezone: Some(timezone))
}

/// Delay each action by a random amount up to `jitter`, to spread load.
pub fn with_jitter(spec: Spec, jitter: Duration) -> Spec {
  Spec(..spec, jitter: Some(jitter))
}

/// A calendar that matches midnight every day: second, minute and hour 0, any date. Narrow
/// or widen it with the `with_` functions below and `exactly`, `between` and `stepping`:
///
/// ```gleam
/// // 09:30 on weekdays
/// schedule.daily()
/// |> schedule.with_hour(schedule.exactly(9))
/// |> schedule.with_minute(schedule.exactly(30))
/// |> schedule.with_day_of_week(schedule.between(1, 5))
/// ```
pub fn daily() -> Calendar {
  Calendar(
    second: exactly(0),
    minute: exactly(0),
    hour: exactly(0),
    day_of_month: between(1, 31),
    month: between(1, 12),
    year: [],
    day_of_week: between(0, 6),
    comment: "",
  )
}

/// The seconds the calendar matches, 0 to 59.
pub fn with_second(calendar: Calendar, ranges: List(Range)) -> Calendar {
  Calendar(..calendar, second: ranges)
}

/// The minutes the calendar matches, 0 to 59.
pub fn with_minute(calendar: Calendar, ranges: List(Range)) -> Calendar {
  Calendar(..calendar, minute: ranges)
}

/// The hours the calendar matches, 0 to 23.
pub fn with_hour(calendar: Calendar, ranges: List(Range)) -> Calendar {
  Calendar(..calendar, hour: ranges)
}

/// The days of the month the calendar matches, 1 to 31.
pub fn with_day_of_month(calendar: Calendar, ranges: List(Range)) -> Calendar {
  Calendar(..calendar, day_of_month: ranges)
}

/// The months the calendar matches, 1 to 12.
pub fn with_month(calendar: Calendar, ranges: List(Range)) -> Calendar {
  Calendar(..calendar, month: ranges)
}

/// The years the calendar matches. An empty list matches every year.
pub fn with_year(calendar: Calendar, ranges: List(Range)) -> Calendar {
  Calendar(..calendar, year: ranges)
}

/// The days of the week the calendar matches, 0 for Sunday to 6 for Saturday.
pub fn with_day_of_week(calendar: Calendar, ranges: List(Range)) -> Calendar {
  Calendar(..calendar, day_of_week: ranges)
}

/// A note stored with the calendar, for whoever reads the schedule later.
pub fn with_comment(calendar: Calendar, comment: String) -> Calendar {
  Calendar(..calendar, comment:)
}

/// The single value `value`.
pub fn exactly(value: Int) -> List(Range) {
  [Range(value, value, 1)]
}

/// Every value from `start` to `end`, inclusive.
pub fn between(start: Int, end: Int) -> List(Range) {
  [Range(start, end, 1)]
}

/// Every `step`th value from `start` to `end`.
pub fn stepping(start: Int, end: Int, step: Int) -> List(Range) {
  [Range(start, end, step)]
}

/// The action that starts `definition` with `input`. Of the start options, the workflow
/// id, task queue, timeouts, retry policy, memo and search attributes apply.
pub fn action(
  definition: workflow.Definition(input, output),
  input: input,
  options: client.StartOptions,
) -> Action {
  from_request(client.start_request(
    definition.name,
    codec.encode_arguments(definition.input, input),
    options,
  ))
}

/// A schedule with the default policies: skip an action that would overlap, not paused.
pub fn new(spec: Spec, action: Action) -> Schedule {
  Schedule(
    spec:,
    action:,
    overlap: Skip,
    catchup_window: None,
    pause_on_failure: False,
    paused: False,
    note: "",
  )
}

/// Replace the schedule's timetable. For `update`.
pub fn with_spec(schedule: Schedule, spec: Spec) -> Schedule {
  Schedule(..schedule, spec:)
}

/// Replace the workflow the schedule starts. For `update`.
pub fn with_action(schedule: Schedule, action: Action) -> Schedule {
  Schedule(..schedule, action:)
}

/// What to do when an action falls due while the previous one is still running.
pub fn with_overlap(schedule: Schedule, overlap: OverlapPolicy) -> Schedule {
  Schedule(..schedule, overlap:)
}

/// How far back to make up for actions missed while the server was down, instead of the
/// server's default of one year.
pub fn with_catchup_window(schedule: Schedule, window: Duration) -> Schedule {
  Schedule(..schedule, catchup_window: Some(window))
}

/// Whether the schedule pauses itself when a workflow it started fails.
pub fn with_pause_on_failure(
  schedule: Schedule,
  pause_on_failure: Bool,
) -> Schedule {
  Schedule(..schedule, pause_on_failure:)
}

/// Whether the schedule is paused. Use it to create a schedule paused; to pause one that
/// exists, use `pause`.
pub fn with_paused(schedule: Schedule, paused: Bool) -> Schedule {
  Schedule(..schedule, paused:)
}

/// A human-readable note, typically why the schedule is paused.
pub fn with_note(schedule: Schedule, note: String) -> Schedule {
  Schedule(..schedule, note:)
}

// ---- conversion ----

fn overlap_value(policy: OverlapPolicy) -> Int {
  case policy {
    Skip -> 1
    BufferOne -> 2
    BufferAll -> 3
    CancelOther -> 4
    TerminateOther -> 5
    AllowAll -> 6
  }
}

fn from_request(request: bridge.StartRequest) -> Action {
  Action(
    workflow_type: request.workflow_type,
    workflow_id: request.workflow_id,
    task_queue: request.task_queue,
    arguments: request.input,
    execution_timeout: time.option_from_ms(request.execution_timeout_ms),
    run_timeout: time.option_from_ms(request.run_timeout_ms),
    task_timeout: time.option_from_ms(request.task_timeout_ms),
    retry: option.map(request.retry, retry.from_bridge),
    memo: request.memo,
    search_attributes: request.search_attributes,
  )
}

fn to_request(action: Action) -> bridge.StartRequest {
  bridge.StartRequest(
    workflow_id: action.workflow_id,
    workflow_type: action.workflow_type,
    task_queue: action.task_queue,
    input: action.arguments,
    id_reuse_policy: 0,
    id_conflict_policy: 0,
    execution_timeout_ms: time.option_to_ms(action.execution_timeout),
    run_timeout_ms: time.option_to_ms(action.run_timeout),
    task_timeout_ms: time.option_to_ms(action.task_timeout),
    retry: option.map(action.retry, retry.to_bridge),
    memo: action.memo,
    search_attributes: action.search_attributes,
    start_delay_ms: None,
    cron_schedule: "",
    request_id: "",
    callbacks: [],
    headers: dict.new(),
  )
}

fn ranges(values: List(Range)) -> List(#(Int, Int, Int)) {
  list.map(values, fn(range) { #(range.start, range.end, range.step) })
}

fn from_ranges(values: List(#(Int, Int, Int))) -> List(Range) {
  list.map(values, fn(range) { Range(range.0, range.1, range.2) })
}

fn to_bridge(client: Client, schedule: Schedule) -> bridge.Schedule {
  let spec = schedule.spec
  bridge.Schedule(
    spec: bridge.ScheduleSpec(
      intervals: list.map(spec.intervals, fn(interval) {
        #(time.to_ms(interval.every), time.to_ms(interval.offset))
      }),
      calendars: list.map(spec.calendars, fn(calendar) {
        bridge.ScheduleCalendar(
          second: ranges(calendar.second),
          minute: ranges(calendar.minute),
          hour: ranges(calendar.hour),
          day_of_month: ranges(calendar.day_of_month),
          month: ranges(calendar.month),
          year: ranges(calendar.year),
          day_of_week: ranges(calendar.day_of_week),
          comment: calendar.comment,
        )
      }),
      cron_expressions: spec.cron_expressions,
      timezone: option.unwrap(spec.timezone, ""),
      jitter_ms: time.option_to_ms(spec.jitter),
    ),
    action: client.encode_request(client, to_request(schedule.action)),
    overlap_policy: overlap_value(schedule.overlap),
    catchup_window_ms: time.option_to_ms(schedule.catchup_window),
    pause_on_failure: schedule.pause_on_failure,
    paused: schedule.paused,
    note: schedule.note,
  )
}

fn from_bridge(client: Client, schedule: bridge.Schedule) -> Schedule {
  let spec = schedule.spec
  let codec = client.payload_codec(client)
  // An action stored without this client's codec is returned as stored.
  let action =
    bridge.StartRequest(
      ..schedule.action,
      input: transcode.decode_list(codec, schedule.action.input)
        |> result.unwrap(schedule.action.input),
      memo: transcode.decode_map(codec, schedule.action.memo)
        |> result.unwrap(schedule.action.memo),
    )
  Schedule(
    spec: Spec(
      intervals: list.map(spec.intervals, fn(interval) {
        Interval(
          every: time.from_ms(interval.0),
          offset: time.from_ms(interval.1),
        )
      }),
      calendars: list.map(spec.calendars, fn(calendar) {
        Calendar(
          second: from_ranges(calendar.second),
          minute: from_ranges(calendar.minute),
          hour: from_ranges(calendar.hour),
          day_of_month: from_ranges(calendar.day_of_month),
          month: from_ranges(calendar.month),
          year: from_ranges(calendar.year),
          day_of_week: from_ranges(calendar.day_of_week),
          comment: calendar.comment,
        )
      }),
      cron_expressions: spec.cron_expressions,
      timezone: case spec.timezone {
        "" -> None
        name -> Some(name)
      },
      jitter: time.option_from_ms(spec.jitter_ms),
    ),
    action: from_request(action),
    overlap: case schedule.overlap_policy {
      2 -> BufferOne
      3 -> BufferAll
      4 -> CancelOther
      5 -> TerminateOther
      6 -> AllowAll
      _ -> Skip
    },
    catchup_window: time.option_from_ms(schedule.catchup_window_ms),
    pause_on_failure: schedule.pause_on_failure,
    paused: schedule.paused,
    note: schedule.note,
  )
}

// ---- requests ----

/// Create a schedule. Fails with `AlreadyExists` if the id is taken.
pub fn create(
  client: Client,
  schedule_id: String,
  schedule: Schedule,
) -> Result(Nil, Error) {
  bridge.schedule_put(
    client.connection(client),
    schedule_id,
    to_bridge(client, schedule),
    None,
    False,
  )
  |> result.map_error(client.request_error)
}

/// The schedule as the server has it, with what it has done and what is due next.
pub fn describe(
  client: Client,
  schedule_id: String,
) -> Result(Description, Error) {
  use #(schedule, info, _) <- result.map(
    bridge.schedule_describe(client.connection(client), schedule_id)
    |> result.map_error(client.request_error),
  )
  Description(
    schedule: from_bridge(client, schedule),
    action_count: info.action_count,
    recent_actions: list.map(info.recent_actions, fn(action) {
      RecentAction(
        scheduled_time: time.timestamp(action.0),
        workflow_id: action.1,
      )
    }),
    next_action_times: list.map(info.next_action_times_ms, time.timestamp),
    running_workflows: info.running_workflows,
  )
}

/// Change a schedule: `change` receives the schedule as the server has it and returns the
/// one to store, usually by passing it through `with_` functions. The update is rejected if the schedule changed in between. Unlike deleting
/// and recreating, this keeps the schedule's history and the timing of its intervals.
pub fn update(
  client: Client,
  schedule_id: String,
  change: fn(Schedule) -> Schedule,
) -> Result(Nil, Error) {
  use #(schedule, _, conflict_token) <- result.try(
    bridge.schedule_describe(client.connection(client), schedule_id)
    |> result.map_error(client.request_error),
  )
  bridge.schedule_put(
    client.connection(client),
    schedule_id,
    to_bridge(client, change(from_bridge(client, schedule))),
    Some(conflict_token),
    False,
  )
  |> result.map_error(client.request_error)
}

/// Delete the schedule. Workflows it has already started carry on.
pub fn delete(client: Client, schedule_id: String) -> Result(Nil, Error) {
  bridge.schedule_delete(client.connection(client), schedule_id)
  |> result.map_error(client.request_error)
}

fn patch(
  client: Client,
  schedule_id: String,
  patch: bridge.SchedulePatch,
) -> Result(Nil, Error) {
  bridge.schedule_patch(client.connection(client), schedule_id, patch)
  |> result.map_error(client.request_error)
}

/// Stop taking actions until unpaused. `note` replaces the schedule's note.
pub fn pause(
  client: Client,
  schedule_id: String,
  note note: String,
) -> Result(Nil, Error) {
  patch(client, schedule_id, bridge.Pause(note))
}

/// Resume taking actions. `note` replaces the schedule's note.
pub fn unpause(
  client: Client,
  schedule_id: String,
  note note: String,
) -> Result(Nil, Error) {
  patch(client, schedule_id, bridge.Unpause(note))
}

/// Take the action now, whatever the timetable says. The schedule's overlap policy applies.
pub fn trigger(client: Client, schedule_id: String) -> Result(Nil, Error) {
  patch(client, schedule_id, bridge.Trigger(0))
}

/// Take the actions the timetable would have taken between `from` and `to`, now. For
/// catching up after an outage, or filling in history for a schedule created late. Most
/// backfills want `AllowAll`, so that the runs do not wait for one another.
pub fn backfill(
  client: Client,
  schedule_id: String,
  from from: Timestamp,
  to to: Timestamp,
  overlap overlap: OverlapPolicy,
) -> Result(Nil, Error) {
  patch(
    client,
    schedule_id,
    bridge.Backfill(
      time.timestamp_to_ms(from),
      time.timestamp_to_ms(to),
      overlap_value(overlap),
    ),
  )
}

/// Every schedule in the namespace. The list lags behind creation and deletion.
pub fn list(client: Client) -> Result(List(Summary), Error) {
  bridge.schedule_list(client.connection(client))
  |> result.map(
    list.map(_, fn(entry) {
      Summary(
        schedule_id: entry.0,
        workflow_type: entry.1,
        paused: entry.2,
        note: entry.3,
      )
    }),
  )
  |> result.map_error(client.request_error)
}
