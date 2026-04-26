//// BEAM scheduler-utilisation and reduction-count collectors.

import gleam/dict
import gleam/dynamic
import gleam/erlang/atom
import gleam/list
import gleeunit/should
import pharos/config
import pharos/measurement
import pharos/metric
import pharos/threshold_eval

pub fn scheduler_utilization_breached_test() -> Nil {
  measurement.BeamScheduler(measurement.SchedulerStats(utilization: 95.0))
  |> threshold_eval.evaluate(config.SchedulerUtilization(above: 90.0))
  |> should.equal(threshold_eval.Breached)
}

pub fn scheduler_utilization_healthy_test() -> Nil {
  measurement.BeamScheduler(measurement.SchedulerStats(utilization: 30.0))
  |> threshold_eval.evaluate(config.SchedulerUtilization(above: 90.0))
  |> should.equal(threshold_eval.Healthy)
}

pub fn scheduler_threshold_id_test() -> Nil {
  config.threshold_id(config.SchedulerUtilization(0.0))
  |> should.equal("scheduler_utilization")
}

pub fn decode_scheduler_event_test() -> Nil {
  let event =
    measurement.TelemetryEvent(
      name: [
        atom.create("pharos"),
        atom.create("vm"),
        atom.create("scheduler"),
      ],
      measurements: dict.from_list([
        #(atom.create("utilization"), dynamic.float(73.0)),
      ]),
      metadata: dict.new(),
    )
  measurement.decode(event)
  |> should.equal(
    Ok(measurement.BeamScheduler(measurement.SchedulerStats(utilization: 73.0))),
  )
}

pub fn decode_reductions_event_test() -> Nil {
  let event =
    measurement.TelemetryEvent(
      name: [
        atom.create("pharos"),
        atom.create("vm"),
        atom.create("reductions"),
      ],
      measurements: dict.from_list([
        #(atom.create("count"), dynamic.int(12_345)),
      ]),
      metadata: dict.new(),
    )
  measurement.decode(event)
  |> should.equal(
    Ok(measurement.BeamReductions(measurement.ReductionStats(count: 12_345))),
  )
}

pub fn metric_of_scheduler_and_reductions_test() -> Nil {
  let sched =
    metric.from_measurement(
      measurement.BeamScheduler(measurement.SchedulerStats(utilization: 50.0)),
    )
  list.map(sched, fn(m) { m.name })
  |> should.equal(["vm.scheduler.utilization"])

  let reductions =
    metric.from_measurement(
      measurement.BeamReductions(measurement.ReductionStats(count: 7)),
    )
  let assert [m] = reductions
  m.name |> should.equal("vm.reductions.count")
  m.value |> should.equal(7.0)
}
