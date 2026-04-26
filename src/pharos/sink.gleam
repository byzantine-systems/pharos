//// The pluggable sink dispatcher
////
//// Each `AlertSink` configured on the `Config` becomes one handler on the
//// alert event bus. The collection/detection logic stays decoupled from the
//// transport: it only ever `notify`s the bus, and every configured sink
//// receives the `AlertEvent` independently. A slow or failing sink cannot
//// block the others (handlers are fire-and-forget).
////
//// Sinks:
//// - **Console** logs locally via `logging` (NixOS `journalctl`).
//// - **Webhook** POSTs a JSON body (e.g. to Slack), the direct-notification
////   path when the Brain is unreachable.
//// - **Brain** ETF-encodes the alert and sends it to a registered process on
////   a (possibly remote) node for native BEAM-to-BEAM delivery.
//// - **Otlp** is a documented stub until the Protobuf/gRPC exporter lands.

import gleam/dict.{type Dict}
import gleam/list
import gleam/result
import logging
import pharos/alert.{
  type AlertEvent, type AlertLevel, AlertFiring, AlertResolved, Critical,
  Warning,
}
import pharos/config.{type AlertSink, Brain, Console, Otlp, Webhook}
import pharos/event_bus.{type EventBus, type HandlerId}
import pharos/internal/connection

/// Attach one bus handler per configured sink. Returns the handler ids of the
/// sinks that attached successfully (a failed attach is skipped, not fatal:
/// the agent still runs with the sinks that did attach).
pub fn attach_all(bus: EventBus, sinks: List(AlertSink)) -> List(HandlerId) {
  list.filter_map(sinks, fn(sink) {
    event_bus.add_handler(bus, handler_for(sink))
    |> result.replace_error(Nil)
  })
}

/// Build the bus handler closure for a single sink.
pub fn handler_for(sink: AlertSink) -> fn(AlertEvent) -> Nil {
  case sink {
    Console(level: level) -> fn(event) { logging.log(level, render(event)) }
    Webhook(url: url) -> fn(event) {
      let _ = webhook_post(url, json_body(event))
      Nil
    }
    Brain(node: node, name: name) -> {
      // Resolve the target's atoms once (not per alert). See DSG-003.
      let target = connection.resolve_brain_target(node, name)
      fn(event) {
        let _ = connection.brain_deliver(target, connection.encode_etf(event))
        Nil
      }
    }
    Otlp(endpoint: endpoint) -> fn(_event) {
      // Stub: the OTLP Protobuf/gRPC exporter is not yet implemented.
      logging.log(
        logging.Debug,
        "pharos otlp sink not yet implemented; dropping alert for " <> endpoint,
      )
    }
  }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Human-readable one-line rendering for the console sink.
pub fn render(event: AlertEvent) -> String {
  case event {
    AlertFiring(id: id, level: level, diagnostic: diagnostic) ->
      "FIRING [" <> level_to_string(level) <> "] " <> id <> " - " <> diagnostic
    AlertResolved(id: id) -> "RESOLVED " <> id
  }
}

/// JSON body for the webhook sink. Built from a string->string map and encoded
/// with Erlang's built-in `json` module (OTP 27+), which handles escaping.
pub fn json_body(event: AlertEvent) -> String {
  json_encode(fields(event))
}

fn fields(event: AlertEvent) -> Dict(String, String) {
  case event {
    AlertFiring(id: id, level: level, diagnostic: diagnostic) ->
      dict.from_list([
        #("status", "firing"),
        #("id", id),
        #("level", level_to_string(level)),
        #("diagnostic", diagnostic),
      ])
    AlertResolved(id: id) ->
      dict.from_list([#("status", "resolved"), #("id", id)])
  }
}

fn level_to_string(level: AlertLevel) -> String {
  case level {
    Warning -> "warning"
    Critical -> "critical"
  }
}

// ---------------------------------------------------------------------------
// FFI
// ---------------------------------------------------------------------------

@external(erlang, "pharos_ffi", "json_encode")
fn json_encode(fields: Dict(String, String)) -> String

@external(erlang, "pharos_ffi", "webhook_post")
fn webhook_post(url: String, body: String) -> Result(Nil, String)
