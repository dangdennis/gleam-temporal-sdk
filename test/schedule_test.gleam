//// Schedules against a real Temporal service.

import gleam/dict
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import support.{cmd, eventually, unique}
import temporal/client
import temporal/codec
import temporal/internal/bridge
import temporal/schedule
import temporal/search_attributes
import temporal/worker
import temporal/workflow

fn tick() {
  workflow.Definition("tick", codec.string(), codec.string())
}

pub fn a_schedule_round_trips_and_can_be_changed_test() {
  let assert Ok(client) = client.connect(client.local())
  let id = "schedule-" <> unique()
  let options =
    client.start_options(workflow_id: id <> "-run", task_queue: id)
    |> client.with_run_timeout(duration.milliseconds(60_000))
    |> client.with_task_timeout(duration.milliseconds(10_000))
    |> client.with_memo(
      dict.from_list([#("origin", codec.encode(codec.string(), "schedule"))]),
    )
  let created =
    schedule.new(
      schedule.every(duration.hours(1))
        |> schedule.with_offset(duration.minutes(7)),
      schedule.action(tick(), "hourly", options),
    )
    |> schedule.with_overlap(schedule.BufferOne)
    |> schedule.with_paused(True)
    |> schedule.with_note("created paused")
  let assert Ok(Nil) = schedule.create(client, id, created)
  let assert Error(client.AlreadyExists(_)) =
    schedule.create(client, id, created)

  let assert Ok(described) = schedule.describe(client, id)
  // The server fills in its default catch-up window of a year.
  assert described.schedule
    == schedule.with_catchup_window(created, duration.hours(365 * 24))
  assert described.action_count == 0
  // Every upcoming action is seven minutes past the hour, as the offset asks.
  assert described.next_action_times != []
  assert list.all(described.next_action_times, fn(time) {
    let #(seconds, _) = timestamp.to_unix_seconds_and_nanoseconds(time)
    seconds % 3600 == 420
  })

  // An update keeps what it does not touch.
  let assert Ok(Nil) =
    schedule.update(client, id, fn(current) {
      schedule.with_spec(
        current,
        schedule.on(
          schedule.daily()
          |> schedule.with_hour(schedule.exactly(9))
          |> schedule.with_minute(schedule.stepping(0, 59, 30))
          |> schedule.with_day_of_week(schedule.between(1, 5))
          |> schedule.with_comment("working hours"),
        )
          |> schedule.with_timezone("Europe/Lisbon"),
      )
    })
  let assert Ok(described) = schedule.describe(client, id)
  let assert [calendar] = described.schedule.spec.calendars
  assert calendar.hour == schedule.exactly(9)
  assert calendar.minute == schedule.stepping(0, 59, 30)
  assert calendar.day_of_week == schedule.between(1, 5)
  assert calendar.comment == "working hours"
  assert described.schedule.spec.intervals == []
  assert described.schedule.spec.timezone == Some("Europe/Lisbon")
  assert described.schedule.overlap == schedule.BufferOne
  assert described.schedule.note == "created paused"
  assert described.schedule.action == created.action

  let assert Ok(Nil) = schedule.unpause(client, id, note: "go")
  let assert Ok(described) = schedule.describe(client, id)
  assert !described.schedule.paused
  assert described.schedule.note == "go"
  assert described.next_action_times != []
  let assert Ok(Nil) = schedule.pause(client, id, note: "stop")
  let assert Ok(described) = schedule.describe(client, id)
  assert described.schedule.paused

  assert eventually(fn() {
    let assert Ok(all) = schedule.list(client)
    list.any(all, fn(summary) {
      summary.schedule_id == id && summary.workflow_type == "tick"
    })
  })

  let assert Ok(Nil) = schedule.delete(client, id)
  let assert Error(client.NotFound(_)) = schedule.describe(client, id)
  let assert Error(client.NotFound(_)) =
    schedule.update(client, id, fn(current) { current })
}

pub fn a_cron_expression_becomes_a_calendar_test() {
  let assert Ok(client) = client.connect(client.local())
  let id = "cron-" <> unique()
  let action =
    schedule.action(
      tick(),
      "cron",
      client.start_options(workflow_id: id, task_queue: id),
    )
  let assert Ok(Nil) =
    schedule.create(
      client,
      id,
      schedule.new(schedule.cron("15 3 * * *"), action),
    )
  let assert Ok(described) = schedule.describe(client, id)
  let assert [calendar] = described.schedule.spec.calendars
  assert calendar.hour == schedule.exactly(3)
  assert calendar.minute == schedule.exactly(15)
  let assert Ok(Nil) = schedule.delete(client, id)
}

pub fn a_triggered_schedule_starts_its_workflow_test() {
  let assert Ok(client) = client.connect(client.local())
  let id = "triggered-" <> unique()
  let attribute = support.register_search_attribute()
  let assert Ok(worker) =
    worker.new(client, id)
    |> worker.with_workflows([
      workflow.handler(tick(), fn(ctx, input) {
        Ok(
          input
          <> " with memo "
          <> option.unwrap(workflow.memo(ctx, "origin", codec.string()), "none"),
        )
      }),
    ])
    |> worker.start
  let options =
    client.start_options(workflow_id: id <> "-run", task_queue: id)
    |> client.with_memo(
      dict.from_list([#("origin", codec.encode(codec.string(), "schedule"))]),
    )
    |> client.with_search_attributes(
      dict.from_list([#(attribute, search_attributes.Keyword(id))]),
    )
  let assert Ok(Nil) =
    schedule.create(
      client,
      id,
      schedule.new(
        schedule.every(duration.hours(24)),
        schedule.action(tick(), "triggered", options),
      ),
    )
  let assert Ok(Nil) = schedule.trigger(client, id)

  assert eventually(fn() {
    let assert Ok(described) = schedule.describe(client, id)
    described.action_count == 1
  })
  let assert Ok(described) = schedule.describe(client, id)
  let assert [action] = described.recent_actions
  // Each run's id is the action's id with the scheduled time appended.
  assert string.starts_with(action.workflow_id, id <> "-run-")
  assert dict.has_key(described.schedule.action.search_attributes, attribute)
  let run = client.handle(client, tick(), action.workflow_id)
  assert client.result(run) == Ok("triggered with memo schedule")

  let assert Ok(Nil) = schedule.delete(client, id)
  worker.stop(worker)
}

/// A schedule with a value in every field `schedule.Schedule` cannot express, as
/// `bridge.schedule_describe` would answer it: what Gleam is shown, and what is stored.
@external(erlang, "temporal_nif", "sample_stored_schedule")
fn sample_stored_schedule() -> #(bridge.Schedule, BitArray)

/// What `bridge.schedule_update` would send to the server.
@external(erlang, "temporal_nif", "debug_schedule_update")
fn debug_schedule_update(
  stored: BitArray,
  described: bridge.Schedule,
  changed: bridge.Schedule,
) -> Result(String, String)

/// What the sample has that this SDK's `Schedule` does not model, as the request prints it.
const unmodelled = [
  "exclude_structured_calendar: [StructuredCalendarSpec",
  "comment: \"maintenance\"", "start_time: Some(Timestamp { seconds: 1000",
  "end_time: Some(Timestamp { seconds: 2000",
  "workflow_id_reuse_policy: RejectDuplicate",
  "header: Some(Header { fields: {\"trace\": [Mg==]} })",
  "keep_original_workflow_id: true", "limited_actions: true",
  "remaining_actions: 5", "conflict_token: [116, 111, 107, 101, 110]",
]

fn assert_contains(text: String, fragments: List(String)) -> Nil {
  list.each(fragments, fn(fragment) {
    case string.contains(text, fragment) {
      True -> Nil
      False -> panic as { "no `" <> fragment <> "` in: " <> text }
    }
  })
}

pub fn an_update_writes_only_what_changed_test() {
  let #(described, stored) = sample_stored_schedule()
  let sent = fn(changed) {
    let assert Ok(sent) = debug_schedule_update(stored, described, changed)
    sent
  }
  let modelled = [
    "interval: Some(Duration { seconds: 3600",
    "phase: Some(Duration { seconds: 60",
    "jitter: Some(Duration { seconds: 1, nanos: 500000000",
    "timezone_name: \"Europe/Lisbon\"", "workflow_id: \"stored-id\"",
    "name: \"stored-type\"", "name: \"stored-queue\"",
    "workflow_run_timeout: Some(Duration { seconds: 90",
    "overlap_policy: BufferOne", "catchup_window: Some(Duration { seconds: 600",
    "pause_on_failure: true", "notes: \"stored note\"",
  ]

  // Nothing changed: the schedule goes back exactly as it is stored.
  let untouched = sent(described)
  assert_contains(untouched, list.append(unmodelled, modelled))

  // One thing changed, far from everything else.
  let noted = sent(bridge.Schedule(..described, note: "new note"))
  assert string.contains(noted, "notes: \"new note\"")
  assert string.replace(noted, "new note", "stored note") == untouched

  // Each part of the schedule, changed: what was not modelled is still there.
  let rebuilt =
    sent(bridge.Schedule(
      spec: bridge.ScheduleSpec(
        intervals: [#(7_200_000, 0)],
        calendars: [],
        cron_expressions: ["0 12 * * *"],
        timezone: "UTC",
        jitter_ms: None,
      ),
      action: bridge.StartRequest(
        ..described.action,
        workflow_id: "new-id",
        workflow_type: "new-type",
        task_queue: "new-queue",
        run_timeout_ms: None,
        execution_timeout_ms: Some(30_000),
      ),
      overlap_policy: 6,
      catchup_window_ms: None,
      pause_on_failure: False,
      paused: True,
      note: "new note",
    ))
  assert_contains(rebuilt, unmodelled)
  assert_contains(rebuilt, [
    "interval: Some(Duration { seconds: 7200", "cron_string: [\"0 12 * * *\"]",
    "timezone_name: \"UTC\"", "jitter: None", "workflow_id: \"new-id\"",
    "name: \"new-type\"", "name: \"new-queue\"", "workflow_run_timeout: None",
    "workflow_execution_timeout: Some(Duration { seconds: 30",
    "overlap_policy: AllowAll", "catchup_window: None",
    "pause_on_failure: false", "paused: true", "notes: \"new note\"",
  ])
}

/// The schedule is made with the Temporal CLI, which can ask for things this SDK's
/// `Schedule` has no field for: run five times, between two dates.
pub fn an_update_keeps_what_the_schedule_type_cannot_express_test() {
  let assert Ok(client) = client.connect(client.local())
  let id = "limited-" <> unique()
  let _ =
    cmd(
      "temporal schedule create --schedule-id "
      <> id
      <> " --interval 1h --workflow-id "
      <> id
      <> "-run --task-queue "
      <> id
      <> " --type tick --input '\"hourly\"' --remaining-actions 5"
      <> " --start-time 2030-01-01T00:00:00Z --end-time 2031-02-03T04:05:06Z"
      <> " --notes 'made by the CLI' 2>&1",
    )
  let stored = fn() {
    cmd("temporal schedule describe --output json --schedule-id " <> id)
  }
  let limits = [
    "\"startTime\": \"2030-01-01T00:00:00Z\"",
    "\"endTime\": \"2031-02-03T04:05:06Z\"", "\"limitedActions\": true",
    "\"remainingActions\": \"5\"",
  ]
  assert_contains(stored(), ["made by the CLI", ..limits])

  let assert Ok(Nil) =
    schedule.update(client, id, schedule.with_note(_, "noted from Gleam"))
  assert_contains(stored(), ["noted from Gleam", "\"3600s\"", ..limits])

  // Changing the timetable itself keeps the dates it runs between.
  let assert Ok(Nil) =
    schedule.update(client, id, fn(current) {
      schedule.with_spec(current, schedule.every(duration.hours(2)))
      |> schedule.with_overlap(schedule.AllowAll)
    })
  assert_contains(stored(), [
    "noted from Gleam",
    "\"7200s\"",
    "SCHEDULE_OVERLAP_POLICY_ALLOW_ALL",
    ..limits
  ])
  let assert Ok(described) = schedule.describe(client, id)
  assert described.schedule.action.workflow_type == "tick"
  let assert Ok(Nil) = schedule.delete(client, id)
}
