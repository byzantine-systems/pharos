//// Host memory threshold evaluation and decoding.

import gleam/dict
import gleam/dynamic
import gleam/erlang/atom
import gleeunit/should
import pharos/config
import pharos/measurement
import pharos/threshold_eval

fn host_memory(
  used: Float,
  status: measurement.HostProbeStatus,
) -> measurement.Measurement {
  measurement.HostMemory(measurement.HostMemoryStats(
    status: status,
    unit: measurement.Mb,
    total: 16_000.0,
    used: used,
    available: 16_000.0 -. used,
  ))
}

pub fn host_memory_used_breached_test() -> Nil {
  host_memory(14_000.0, measurement.Implemented)
  |> threshold_eval.evaluate(config.HostMemoryUsed(above: 12_000.0))
  |> should.equal(threshold_eval.Breached)
}

pub fn host_memory_used_healthy_test() -> Nil {
  host_memory(8000.0, measurement.Implemented)
  |> threshold_eval.evaluate(config.HostMemoryUsed(above: 12_000.0))
  |> should.equal(threshold_eval.Healthy)
}

pub fn host_memory_unimplemented_is_not_applicable_test() -> Nil {
  // A host probe still reporting `Unimplemented` must never breach/recover,
  // even if its placeholder values happen to clear the limit.
  host_memory(14_000.0, measurement.Unimplemented)
  |> threshold_eval.evaluate(config.HostMemoryUsed(above: 12_000.0))
  |> should.equal(threshold_eval.NotApplicable)
}

pub fn host_memory_threshold_against_beam_measurement_is_not_applicable_test() -> Nil {
  measurement.BeamMemory(measurement.BeamMemoryStats(
    unit: measurement.Mb,
    total: 100.0,
    processes: 0.0,
    processes_used: 0.0,
    system: 0.0,
    atom: 0.0,
    atom_used: 0.0,
    binary: 0.0,
    code: 0.0,
    ets: 0.0,
  ))
  |> threshold_eval.evaluate(config.HostMemoryUsed(above: 1.0))
  |> should.equal(threshold_eval.NotApplicable)
}

pub fn host_memory_threshold_id_test() -> Nil {
  config.threshold_id(config.HostMemoryUsed(0.0))
  |> should.equal("host_memory_used")
}

pub fn decode_host_memory_event_test() -> Nil {
  // `[pharos, host, memory]` with real byte counts and no `status` decodes as
  // an `Implemented` host measurement, with bytes scaled to the unit (Mb).
  let one_mb = 1_048_576
  let event =
    measurement.TelemetryEvent(
      name: [atom.create("pharos"), atom.create("host"), atom.create("memory")],
      measurements: dict.from_list([
        #(atom.create("total_bytes"), dynamic.int(16 * one_mb)),
        #(atom.create("used_bytes"), dynamic.int(10 * one_mb)),
        #(atom.create("available_bytes"), dynamic.int(6 * one_mb)),
      ]),
      metadata: dict.new(),
    )
  measurement.decode_with_unit(event, measurement.Mb)
  |> should.equal(
    Ok(
      measurement.HostMemory(measurement.HostMemoryStats(
        status: measurement.Implemented,
        unit: measurement.Mb,
        total: 16.0,
        used: 10.0,
        available: 6.0,
      )),
    ),
  )
}
