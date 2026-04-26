//// Host disk threshold evaluation and decoding.

import gleam/dict
import gleam/dynamic
import gleam/erlang/atom
import gleeunit/should
import pharos/config
import pharos/measurement
import pharos/threshold_eval

fn host_disk(
  used: Float,
  status: measurement.HostProbeStatus,
) -> measurement.Measurement {
  measurement.HostDisk(measurement.HostDiskStats(
    status: status,
    unit: measurement.Mb,
    total: 100_000.0,
    used: used,
    available: 100_000.0 -. used,
  ))
}

pub fn host_disk_used_breached_test() -> Nil {
  host_disk(90_000.0, measurement.Implemented)
  |> threshold_eval.evaluate(config.HostDiskUsed(above: 80_000.0))
  |> should.equal(threshold_eval.Breached)
}

pub fn host_disk_used_healthy_test() -> Nil {
  host_disk(40_000.0, measurement.Implemented)
  |> threshold_eval.evaluate(config.HostDiskUsed(above: 80_000.0))
  |> should.equal(threshold_eval.Healthy)
}

pub fn host_disk_unimplemented_is_not_applicable_test() -> Nil {
  host_disk(90_000.0, measurement.Unimplemented)
  |> threshold_eval.evaluate(config.HostDiskUsed(above: 80_000.0))
  |> should.equal(threshold_eval.NotApplicable)
}

pub fn host_disk_threshold_id_test() -> Nil {
  config.threshold_id(config.HostDiskUsed(0.0))
  |> should.equal("host_disk_used")
}

pub fn decode_host_disk_event_test() -> Nil {
  let one_mb = 1_048_576
  let event =
    measurement.TelemetryEvent(
      name: [atom.create("pharos"), atom.create("host"), atom.create("disk")],
      measurements: dict.from_list([
        #(atom.create("total_bytes"), dynamic.int(100 * one_mb)),
        #(atom.create("used_bytes"), dynamic.int(60 * one_mb)),
        #(atom.create("available_bytes"), dynamic.int(40 * one_mb)),
      ]),
      metadata: dict.new(),
    )
  measurement.decode_with_unit(event, measurement.Mb)
  |> should.equal(
    Ok(
      measurement.HostDisk(measurement.HostDiskStats(
        status: measurement.Implemented,
        unit: measurement.Mb,
        total: 100.0,
        used: 60.0,
        available: 40.0,
      )),
    ),
  )
}
