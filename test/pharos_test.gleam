import gleam/dict
import gleam/erlang/atom
import gleeunit
import gleeunit/should
import pharos
import pharos/alert
import pharos/config
import pharos/measurement
import pharos/probe
import pharos/statistic
import pharos/threshold_eval

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn start_link_returns_ok_test() -> Nil {
  let assert Ok(started) =
    pharos.start_link(
      config.new()
      |> config.with_statistics([statistic.poll(statistic.BeamMemory)]),
    )
  pharos.stop(started.data)
}

pub fn start_link_with_no_statistics_succeeds_test() -> Nil {
  let assert Ok(started) = pharos.start_link(config.new())
  pharos.stop(started.data)
}

pub fn subscribe_returns_handler_id_test() -> Nil {
  let assert Ok(started) = pharos.start_link(config.new())
  pharos.subscribe(started.data, fn(_event) { Nil })
  |> should.be_ok
  pharos.stop(started.data)
}

// ---------------------------------------------------------------------------
// Custom probe lane
// ---------------------------------------------------------------------------

fn sample_probe() -> probe.Probe {
  probe.new(
    id: "vm",
    event_name: [
      atom.create("logothetes"),
      atom.create("vm"),
      atom.create("metrics"),
    ],
    interval_ms: 600_000,
    source: probe.CustomMfa(
      module: atom.create("pharos_ffi"),
      function: atom.create("emit_cluster_nodes"),
      args: [],
    ),
    decode: fn(_event) { Ok(dict.from_list([#("memory_mb", 128.0)])) },
  )
}

pub fn probe_decode_invokes_decoder_test() -> Nil {
  let p = sample_probe()
  let event =
    measurement.TelemetryEvent(
      name: probe.event_name(p),
      measurements: dict.new(),
      metadata: dict.new(),
    )
  probe.decode(p, event)
  |> should.equal(Ok(dict.from_list([#("memory_mb", 128.0)])))
}

pub fn probe_accessors_test() -> Nil {
  let p = sample_probe()
  probe.id(p) |> should.equal("vm")
  probe.interval_ms(p) |> should.equal(600_000)
}

pub fn probe_threshold_id_test() -> Nil {
  probe.ProbeThreshold(
    probe_id: "vm",
    field: "memory_mb",
    comparison: probe.Above(500.0),
    level: alert.Warning,
  )
  |> probe.threshold_id
  |> should.equal("vm_memory_mb_above")
}

pub fn evaluate_probe_above_test() -> Nil {
  let sample = dict.from_list([#("memory_mb", 600.0)])
  threshold_eval.evaluate_probe(
    sample,
    probe.ProbeThreshold("vm", "memory_mb", probe.Above(500.0), alert.Warning),
  )
  |> should.equal(threshold_eval.Breached)
  threshold_eval.evaluate_probe(
    sample,
    probe.ProbeThreshold("vm", "memory_mb", probe.Above(800.0), alert.Warning),
  )
  |> should.equal(threshold_eval.Healthy)
}

pub fn evaluate_probe_below_test() -> Nil {
  let sample = dict.from_list([#("free_mb", 64.0)])
  threshold_eval.evaluate_probe(
    sample,
    probe.ProbeThreshold("vm", "free_mb", probe.Below(128.0), alert.Warning),
  )
  |> should.equal(threshold_eval.Breached)
  threshold_eval.evaluate_probe(
    sample,
    probe.ProbeThreshold("vm", "free_mb", probe.Below(32.0), alert.Warning),
  )
  |> should.equal(threshold_eval.Healthy)
}

pub fn evaluate_probe_missing_field_is_not_applicable_test() -> Nil {
  threshold_eval.evaluate_probe(
    dict.new(),
    probe.ProbeThreshold("vm", "memory_mb", probe.Above(1.0), alert.Warning),
  )
  |> should.equal(threshold_eval.NotApplicable)
}

pub fn start_link_with_custom_probe_succeeds_test() -> Nil {
  let threshold =
    probe.ProbeThreshold(
      probe_id: "vm",
      field: "memory_mb",
      comparison: probe.Above(512.0),
      level: alert.Warning,
    )
  let assert Ok(started) =
    pharos.start_link(
      config.new()
      |> config.with_custom_statistics([sample_probe()])
      |> config.with_custom_thresholds([threshold]),
    )
  pharos.subscribe(started.data, fn(_event) { Nil })
  |> should.be_ok
  pharos.stop(started.data)
}
