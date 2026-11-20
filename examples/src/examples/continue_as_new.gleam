//// Continue-as-new: a long-running loop whose history stays small.
////
//// A workflow's history grows with everything it does, and a run is limited to about
//// fifty thousand events. A workflow that loops for a long time ends each run after a
//// bounded amount of work and starts the next with the state it wants to keep.

import gleam/int
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/schema
import temporal/worker
import temporal/workflow

const task_queue = "examples-continue-as-new"

/// Pages handled in one run before continuing as new.
const pages_per_run = 3

/// Everything the next run needs to carry on.
pub type Progress {
  Progress(next_page: Int, last_page: Int, rows: Int)
}

fn progress_codec() {
  schema.codec(schema.object3(
    Progress,
    schema.field("nextPage", schema.int(), fn(p: Progress) { p.next_page }),
    schema.field("lastPage", schema.int(), fn(p: Progress) { p.last_page }),
    schema.field("rows", schema.int(), fn(p: Progress) { p.rows }),
  ))
}

pub fn sync_pages() {
  workflow.Definition("sync_pages", progress_codec(), codec.int())
}

/// Copies one page and returns how many rows it held.
pub fn copy_page() {
  activity.Definition("copy_page", codec.int(), codec.int())
}

fn sync_pages_workflow(
  ctx: workflow.Context,
  progress: Progress,
) -> Result(Int, Failure) {
  // This run's share of the work: a few pages, or what is left.
  let stop = int.min(progress.next_page + pages_per_run, progress.last_page + 1)
  use rows <- result.try(copy_pages(
    ctx,
    progress.next_page,
    stop,
    progress.rows,
  ))
  case stop > progress.last_page {
    True -> Ok(rows)
    // Ends this run and starts a new one with an empty history. Does not return.
    // `workflow.info(ctx).continue_as_new_suggested` is the server's own hint that a
    // history has grown long, for loops with no natural batch size. The new run keeps
    // the task queue, memo, search attributes and headers of this one; to change them
    // or set timeouts, use `temporal/workflow/continue_as_new`.
    False ->
      workflow.continue_as_new(
        ctx,
        sync_pages(),
        Progress(..progress, next_page: stop, rows:),
      )
  }
}

/// Copy the pages from `page` up to, not including, `stop`, adding to `rows`.
fn copy_pages(
  ctx: workflow.Context,
  page: Int,
  stop: Int,
  rows: Int,
) -> Result(Int, Failure) {
  case page >= stop {
    True -> Ok(rows)
    False -> {
      let options =
        workflow.activity_options(start_to_close: duration.seconds(10))
      use copied <- result.try(workflow.execute_activity(
        ctx,
        copy_page(),
        page,
        options,
      ))
      copy_pages(ctx, page + 1, stop, rows + copied)
    }
  }
}

fn copy_page_activity(
  _ctx: activity.Context,
  _page: Int,
) -> Result(Int, Failure) {
  Ok(100)
}

pub fn run(client: Client) -> Result(Int, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(sync_pages(), sync_pages_workflow),
    ])
    |> worker.with_activities([
      activity.handler(copy_page(), copy_page_activity),
    ])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-continue-as-new", task_queue:)
  // Ten pages take four runs. The result is the last run's: `execute` follows the chain.
  let rows =
    client.execute(
      client,
      sync_pages(),
      Progress(next_page: 1, last_page: 10, rows: 0),
      options,
    )
  worker.stop(worker)
  rows
}
