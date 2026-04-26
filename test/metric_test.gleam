//// Metric flattening.

import gleam/dict
import gleam/list
import gleeunit/should
import pharos/measurement
import pharos/metric

pub fn metric_of_measurement_beam_memory_test() -> Nil {
  let memory =
    measurement.BeamMemory(measurement.BeamMemoryStats(
      unit: measurement.Mb,
      total: 100.0,
      processes: 10.0,
      processes_used: 5.0,
      system: 20.0,
      atom: 1.0,
      atom_used: 1.0,
      binary: 2.0,
      code: 3.0,
      ets: 4.0,
    ))
  let metrics = metric.from_measurement(memory)
  list.length(metrics) |> should.equal(7)
  let names = list.map(metrics, fn(m) { m.name })
  list.contains(names, "vm.memory.total") |> should.be_true
  list.contains(names, "vm.memory.ets") |> should.be_true
}

pub fn metric_of_measurement_unimplemented_host_is_empty_test() -> Nil {
  measurement.HostMemory(measurement.HostMemoryStats(
    status: measurement.Unimplemented,
    unit: measurement.Mb,
    total: 0.0,
    used: 0.0,
    available: 0.0,
  ))
  |> metric.from_measurement
  |> should.equal([])
}

pub fn metric_of_sample_test() -> Nil {
  let assert [m] =
    metric.from_sample("vm", dict.from_list([#("memory_mb", 128.0)]))
  m.name |> should.equal("probe.vm.memory_mb")
  m.value |> should.equal(128.0)
}
