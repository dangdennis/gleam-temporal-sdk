//// Schedules: start a workflow on a timetable.
////
//// Use a schedule instead of a cron job or a workflow that loops and sleeps. The server
//// keeps the timetable, so nothing has to be running when an action falls due, and a
//// schedule can be paused, triggered by hand and backfilled.

import gleam/erlang/process
import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/schedule
import temporal/worker
import temporal/workflow

const task_queue = "examples-schedules"

pub const schedule_id = "examples-schedules"

/// What `run` observed.
pub type Outcome {
  Outcome(
    /// Whether the schedule reported itself paused after `pause`, and its note.
    paused: Bool,
    note: String,
    /// Actions taken: one triggered by hand and two backfilled.
    actions: Int,
  )
}

pub fn send_report() {
  workflow.Definition("send_report", codec.string(), codec.nil())
}

fn send_report_workflow(
  _ctx: workflow.Context,
  _team: String,
) -> Result(Nil, Failure) {
  Ok(Nil)
}

pub fn run(client: Client) -> Result(Outcome, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(send_report(), send_report_workflow),
    ])
    |> worker.start,
  )
  let hourly =
    schedule.new(
      // Every hour, at ten past. `schedule.cron` and `schedule.on` take a cron expression
      // or a calendar instead.
      schedule.every(duration.hours(1))
        |> schedule.with_offset(duration.minutes(10)),
      // The workflow to start. The server appends the scheduled time to the id.
      schedule.action(
        send_report(),
        "platform",
        client.start_options(
          workflow_id: "examples-schedules-report",
          task_queue:,
        ),
      ),
    )
    |> schedule.with_overlap(schedule.BufferOne)
    |> schedule.with_note("hourly report")

  let outcome = {
    use _ <- result.try(schedule.create(client, schedule_id, hourly))

    // Pause and resume, with a note saying why.
    use _ <- result.try(schedule.pause(client, schedule_id, note: "maintenance"))
    use described <- result.try(schedule.describe(client, schedule_id))
    let paused = described.schedule.paused
    let note = described.schedule.note
    use _ <- result.try(schedule.unpause(client, schedule_id, note: "back"))

    // Take the action now, whatever the timetable says.
    use _ <- result.try(schedule.trigger(client, schedule_id))
    use _ <- result.try(wait_for_actions(client, 1, 50))

    // Take the actions the timetable would have taken over a past period: here two.
    let now = timestamp.system_time()
    use _ <- result.try(schedule.backfill(
      client,
      schedule_id,
      from: timestamp.add(now, duration.hours(-3)),
      to: timestamp.add(now, duration.hours(-1)),
      overlap: schedule.AllowAll,
    ))
    use actions <- result.try(wait_for_actions(client, 3, 50))

    // Workflows the schedule already started carry on.
    use _ <- result.try(schedule.delete(client, schedule_id))
    Ok(Outcome(paused:, note:, actions:))
  }
  worker.stop(worker)
  outcome
}

/// Poll until the schedule has taken `count` actions: it takes them shortly after it is
/// asked to. Returns the count seen last.
fn wait_for_actions(
  client: Client,
  count: Int,
  tries: Int,
) -> Result(Int, client.Error) {
  use described <- result.try(schedule.describe(client, schedule_id))
  case described.action_count >= count || tries <= 0 {
    True -> Ok(described.action_count)
    False -> {
      process.sleep(200)
      wait_for_actions(client, count, tries - 1)
    }
  }
}
