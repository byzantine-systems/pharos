//// Host CPU threshold evaluation, decoding, and metric flattening.

import gleam/dict
import gleam/dynamic
import gleam/erlang/atom
import gleam/list
import gleeunit/should
import pharos/config
import pharos/measurement
import pharos/metric
import pharos/threshold_eval

fn host_cpu(util: Float) -> measurement.Measurement {
  measurement.HostCpu(measurement.HostCpuStats(
    util: util,
    load1: 1.0,
    load5: 0.5,
    load15: 0.25,
  ))
}

pub fn host_cpu_util_breached_test() -> Nil {
  host_cpu(92.0)
  |> threshold_eval.evaluate(config.HostCpuUtil(above: 80.0))
  |> should.equal(threshold_eval.Breached)
}

pub fn host_cpu_util_healthy_test() -> Nil {
  host_cpu(20.0)
  |> threshold_eval.evaluate(config.HostCpuUtil(above: 80.0))
  |> should.equal(threshold_eval.Healthy)
}

pub fn host_cpu_threshold_against_other_measurement_is_not_applicable_test() -> Nil {
  host_cpu(92.0)
  |> threshold_eval.evaluate(config.HostMemoryUsed(above: 1.0))
  |> should.equal(threshold_eval.NotApplicable)
}

pub fn host_cpu_threshold_id_test() -> Nil {
  config.threshold_id(config.HostCpuUtil(0.0))
  |> should.equal("host_cpu_util")
}

pub fn decode_host_cpu_event_test() -> Nil {
  // util arrives as a float; load averages too. `get_number` also accepts ints.
  let event =
    measurement.TelemetryEvent(
      name: [atom.create("pharos"), atom.create("host"), atom.create("cpu")],
      measurements: dict.from_list([
        #(atom.create("util"), dynamic.float(42.5)),
        #(atom.create("load1"), dynamic.float(1.5)),
        #(atom.create("load5"), dynamic.float(0.75)),
        #(atom.create("load15"), dynamic.int(0)),
      ]),
      metadata: dict.new(),
    )
  measurement.decode(event)
  |> should.equal(
    Ok(
      measurement.HostCpu(measurement.HostCpuStats(
        util: 42.5,
        load1: 1.5,
        load5: 0.75,
        load15: 0.0,
      )),
    ),
  )
}

pub fn metric_of_host_cpu_test() -> Nil {
  let names =
    list.map(metric.from_measurement(host_cpu(50.0)), fn(m) { m.name })
  names
  |> should.equal([
    "host.cpu.util", "host.cpu.load1", "host.cpu.load5", "host.cpu.load15",
  ])
}
