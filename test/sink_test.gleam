//// Sinks

import gleam/erlang/process
import gleam/list
import gleam/string
import gleeunit/should
import logging
import pharos/alert
import pharos/config
import pharos/event_bus
import pharos/sink

pub fn sink_render_firing_test() -> Nil {
  sink.render(alert.AlertFiring(
    id: "cpu",
    level: alert.Critical,
    diagnostic: "hot",
  ))
  |> should.equal("FIRING [critical] cpu - hot")
}

pub fn sink_render_resolved_test() -> Nil {
  sink.render(alert.AlertResolved(id: "cpu")) |> should.equal("RESOLVED cpu")
}

pub fn sink_json_body_firing_test() -> Nil {
  let body =
    sink.json_body(alert.AlertFiring(
      id: "cpu",
      level: alert.Warning,
      diagnostic: "d",
    ))
  string.contains(body, "\"status\":\"firing\"") |> should.be_true
  string.contains(body, "\"id\":\"cpu\"") |> should.be_true
  string.contains(body, "\"level\":\"warning\"") |> should.be_true
}

pub fn sink_attach_all_console_test() -> Nil {
  let assert Ok(#(_pid, bus)) =
    event_bus.start_link(process.new_name("pharos_test_sink_bus"))
  let ids = sink.attach_all(bus, [config.Console(level: logging.Info)])
  list.length(ids) |> should.equal(1)
  // Delivering through the configured sink must not crash.
  event_bus.notify(
    bus,
    alert.AlertFiring(id: "x", level: alert.Warning, diagnostic: "d"),
  )
}
