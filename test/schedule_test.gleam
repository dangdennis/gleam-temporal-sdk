//// Schedules against a real Temporal service.

import gleam/dict
import gleam/list
import gleam/option.{Some}
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import support.{eventually, unique}
import temporal/client
import temporal/codec
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
    client.StartOptions(
      ..client.start_options(workflow_id: id <> "-run", task_queue: id),
      run_timeout: Some(duration.milliseconds(60_000)),
      task_timeout: Some(duration.milliseconds(10_000)),
      memo: dict.from_list([
        #("origin", codec.encode(codec.string(), "schedule")),
      ]),
    )
  let created =
    schedule.Schedule(
      ..schedule.new(
        schedule.every_offset(duration.hours(1), offset: duration.minutes(7)),
        schedule.action(tick(), "hourly", options),
      ),
      overlap: schedule.BufferOne,
      paused: True,
      note: "created paused",
    )
  let assert Ok(Nil) = schedule.create(client, id, created)
  let assert Error(client.AlreadyExists(_)) =
    schedule.create(client, id, created)

  let assert Ok(described) = schedule.describe(client, id)
  // The server fills in its default catch-up window of a year.
  assert described.schedule
    == schedule.Schedule(
      ..created,
      catchup_window: Some(duration.hours(365 * 24)),
    )
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
      schedule.Schedule(
        ..current,
        spec: schedule.on(
            schedule.Calendar(
              ..schedule.daily(),
              hour: schedule.exactly(9),
              minute: schedule.stepping(0, 59, 30),
              day_of_week: schedule.between(1, 5),
              comment: "working hours",
            ),
          )
          |> schedule.in_timezone("Europe/Lisbon"),
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
    client.StartOptions(
      ..client.start_options(workflow_id: id <> "-run", task_queue: id),
      memo: dict.from_list([
        #("origin", codec.encode(codec.string(), "schedule")),
      ]),
      search_attributes: dict.from_list([
        #(attribute, search_attributes.Keyword(id)),
      ]),
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
