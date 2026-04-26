//// Sliding-window threshold evaluation.

import gleam/erlang/process
import gleam/option.{Some}
import gleeunit/should
import pharos/alert
import pharos/alert_manager
import pharos/config
import pharos/event_bus
import pharos/measurement
import pharos/threshold_eval

// ---------------------------------------------------------------------------
// Pure aggregation: alert.window_breached
// ---------------------------------------------------------------------------

pub fn window_average_breached_test() -> Nil {
  let spec = alert.WindowSpec(window_ms: 1000, limit: 50.0, mode: alert.Average)
  // mean(100) = 100 > 50
  alert.window_breached([#(0, 100.0)], 0, spec) |> should.be_true
  // mean(100, 0) = 50, not > 50
  alert.window_breached([#(0, 100.0), #(0, 0.0)], 0, spec) |> should.be_false
}

pub fn window_percentage_boundary_test() -> Nil {
  let samples = [#(0, 100.0), #(0, 100.0), #(0, 0.0)]
  // 2 of 3 samples exceed 50 -> 0.667
  alert.window_breached(
    samples,
    0,
    alert.WindowSpec(1000, 50.0, alert.Percentage(at_least: 0.5)),
  )
  |> should.be_true
  alert.window_breached(
    samples,
    0,
    alert.WindowSpec(1000, 50.0, alert.Percentage(at_least: 0.7)),
  )
  |> should.be_false
}

pub fn window_prunes_old_samples_test() -> Nil {
  // The lone sample is older than now - window_ms, so the window is empty.
  let spec = alert.WindowSpec(window_ms: 1000, limit: 50.0, mode: alert.Average)
  alert.window_breached([#(0, 100.0)], 2000, spec) |> should.be_false
}

pub fn window_empty_is_not_breached_test() -> Nil {
  alert.window_breached([], 0, alert.WindowSpec(1000, 0.0, alert.Average))
  |> should.be_false
}

// ---------------------------------------------------------------------------
// threshold_eval: sample_value / metric_name / threshold_limit
// ---------------------------------------------------------------------------

fn host_cpu(util: Float) -> measurement.Measurement {
  measurement.HostCpu(measurement.HostCpuStats(
    util: util,
    load1: 0.0,
    load5: 0.0,
    load15: 0.0,
  ))
}

pub fn sample_value_extracts_field_test() -> Nil {
  threshold_eval.sample_value(host_cpu(42.0), config.HostCpuUtil(above: 0.0))
  |> should.equal(Ok(42.0))
}

pub fn sample_value_wrong_measurement_is_error_test() -> Nil {
  threshold_eval.sample_value(host_cpu(42.0), config.TotalMemory(above: 0.0))
  |> should.equal(Error(Nil))
}

pub fn sample_value_unimplemented_host_is_error_test() -> Nil {
  // An Unimplemented host probe flattens to no metrics -> no sample.
  measurement.HostMemory(measurement.HostMemoryStats(
    status: measurement.Unimplemented,
    unit: measurement.Mb,
    total: 0.0,
    used: 0.0,
    available: 0.0,
  ))
  |> threshold_eval.sample_value(config.HostMemoryUsed(above: 0.0))
  |> should.equal(Error(Nil))
}

pub fn metric_name_and_limit_test() -> Nil {
  threshold_eval.metric_name(config.HostCpuUtil(above: 0.0))
  |> should.equal("host.cpu.util")
  // Windowed delegates to its inner threshold.
  threshold_eval.metric_name(config.Windowed(
    config.TotalMemory(above: 0.0),
    1000,
    alert.Average,
  ))
  |> should.equal("vm.memory.total")
  // Integer limits widen to Float.
  threshold_eval.threshold_limit(config.ProcessCount(above: 100))
  |> should.equal(100.0)
  threshold_eval.threshold_limit(config.HostCpuUtil(above: 80.0))
  |> should.equal(80.0)
}

// ---------------------------------------------------------------------------
// FSM integration: windowed alert_manager fires and resolves
// ---------------------------------------------------------------------------

pub fn windowed_manager_fires_then_resolves_test() -> Nil {
  let events = process.new_subject()
  let bus_name = process.new_name("pharos_test_window_bus")
  let assert Ok(_) = event_bus.start_link(bus_name)
  let bus = event_bus.from_name(bus_name)
  let assert Ok(_) =
    event_bus.add_handler(bus, fn(event) { process.send(events, event) })

  let data =
    alert.AlertData(
      id: "windowed_host_cpu_util",
      level: alert.Warning,
      // Window is the debounce; cool kept short for the test.
      soak_period_ms: 0,
      cool_period_ms: 50,
      window: Some(alert.WindowSpec(
        window_ms: 10_000,
        limit: 50.0,
        mode: alert.Average,
      )),
    )
  let name = process.new_name("pharos_test_window_mgr")
  let assert Ok(manager) = alert_manager.start_link(data, bus, name)

  // A sample above the limit pushes the window mean over 50 -> fires now.
  alert_manager.sample(manager, 100.0)
  let assert Ok(alert.AlertFiring(id: "windowed_host_cpu_util", ..)) =
    process.receive(events, 1000)

  // A healthy sample pulls the mean to 50 (not > 50) -> recover -> resolves
  // after the cool period.
  alert_manager.sample(manager, 0.0)
  let assert Ok(alert.AlertResolved(id: "windowed_host_cpu_util")) =
    process.receive(events, 1000)
  Nil
}
