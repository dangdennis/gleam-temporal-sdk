//// Data: describing the values a workflow sends and receives.
////
//// Every definition carries a codec for its input and its output. Use `temporal/schema`
//// for JSON objects: one description gives the encoder and the decoder. Use the
//// argument-list codecs for several arguments or none, and `codec.json` with a
//// hand-written pair for what a schema cannot describe, such as a type with several
//// variants.

import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, Some}
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/schema
import temporal/worker
import temporal/workflow

const task_queue = "examples-data"

// ---- objects, nested and optional fields ----

pub type Customer {
  Customer(name: String, email: Option(String))
}

pub type Line {
  Line(sku: String, quantity: Int)
}

pub type Order {
  Order(id: String, customer: Customer, lines: List(Line))
}

pub type Invoice {
  Invoice(order_id: String, total_cents: Int, sent_to: Option(String))
}

/// Fields are given in the order of the constructor's arguments. The JSON names are
/// whatever the other side expects; they need not match the Gleam ones.
fn customer_schema() {
  schema.object2(
    Customer,
    schema.field("name", schema.string(), fn(c: Customer) { c.name }),
    // Written as null when `None`; null or a missing field reads as `None`.
    schema.field("email", schema.optional(schema.string()), fn(c: Customer) {
      c.email
    }),
  )
}

fn line_schema() {
  schema.object2(
    Line,
    schema.field("sku", schema.string(), fn(l: Line) { l.sku }),
    schema.field("quantity", schema.int(), fn(l: Line) { l.quantity }),
  )
}

/// Schemas nest: a field's schema may be another object, a list or a dict.
fn order_schema() {
  schema.object3(
    Order,
    schema.field("id", schema.string(), fn(o: Order) { o.id }),
    schema.field("customer", customer_schema(), fn(o: Order) { o.customer }),
    schema.field("lines", schema.list(line_schema()), fn(o: Order) { o.lines }),
  )
}

fn invoice_schema() {
  schema.object3(
    Invoice,
    schema.field("orderId", schema.string(), fn(i: Invoice) { i.order_id }),
    schema.field("totalCents", schema.int(), fn(i: Invoice) { i.total_cents }),
    schema.field("sentTo", schema.optional(schema.string()), fn(i: Invoice) {
      i.sent_to
    }),
  )
}

// ---- a type with variants, by hand ----

pub type Discount {
  NoDiscount
  Percent(Int)
}

fn discount_codec() {
  codec.json(
    fn(discount) {
      case discount {
        NoDiscount -> json.object([#("kind", json.string("none"))])
        Percent(percent) ->
          json.object([
            #("kind", json.string("percent")),
            #("percent", json.int(percent)),
          ])
      }
    },
    {
      use kind <- decode.field("kind", decode.string)
      case kind {
        "percent" -> {
          use percent <- decode.field("percent", decode.int)
          decode.success(Percent(percent))
        }
        "none" -> decode.success(NoDiscount)
        _ -> decode.failure(NoDiscount, "Discount")
      }
    },
  )
}

// ---- definitions ----

/// Two arguments, passed as a tuple. Other SDKs see an ordinary two-argument workflow.
pub fn invoice_order() {
  workflow.Definition(
    "invoice_order",
    codec.args2(schema.codec(order_schema()), discount_codec()),
    schema.codec(invoice_schema()),
  )
}

/// No arguments, and a JSON object used as a map for a result.
pub fn price_list() {
  activity.Definition(
    "price_list",
    codec.no_args(),
    schema.codec(schema.dict(schema.int())),
  )
}

fn invoice_order_workflow(
  ctx: workflow.Context,
  input: #(Order, Discount),
) -> Result(Invoice, Failure) {
  let #(order, discount) = input
  let options = workflow.activity_options(start_to_close: duration.seconds(10))
  use prices <- result.try(workflow.execute_activity(
    ctx,
    price_list(),
    Nil,
    options,
  ))
  let subtotal = int.sum(list.map(order.lines, line_total(prices, _)))
  let total = case discount {
    NoDiscount -> subtotal
    Percent(percent) -> subtotal * { 100 - percent } / 100
  }
  Ok(Invoice(
    order_id: order.id,
    total_cents: total,
    sent_to: order.customer.email,
  ))
}

fn line_total(prices: Dict(String, Int), line: Line) -> Int {
  line.quantity * { dict.get(prices, line.sku) |> result.unwrap(0) }
}

fn price_list_activity(
  _ctx: activity.Context,
  _: Nil,
) -> Result(Dict(String, Int), Failure) {
  Ok(dict.from_list([#("pen", 150), #("ink", 900)]))
}

pub fn run(client: Client) -> Result(Invoice, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(invoice_order(), invoice_order_workflow),
    ])
    |> worker.with_activities([
      activity.handler(price_list(), price_list_activity),
    ])
    |> worker.start,
  )
  let order =
    Order(
      id: "order-7",
      customer: Customer(name: "Ada", email: Some("ada@example.com")),
      lines: [Line(sku: "pen", quantity: 4), Line(sku: "ink", quantity: 1)],
    )
  let options = client.start_options(workflow_id: "examples-data", task_queue:)
  let invoice =
    client.execute(client, invoice_order(), #(order, Percent(10)), options)
  worker.stop(worker)
  invoice
}
