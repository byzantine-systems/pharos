//// Host network decoding and metric flattening.

import gleam/dict
import gleam/dynamic
import gleam/erlang/atom
import gleam/list
import gleeunit/should
import pharos/measurement
import pharos/metric

fn host_network() -> measurement.Measurement {
  measurement.HostNetwork(measurement.HostNetworkStats(
    rx_bytes_per_sec: 1000.0,
    tx_bytes_per_sec: 500.0,
    rx_packets_per_sec: 10.0,
    tx_packets_per_sec: 5.0,
  ))
}

pub fn decode_host_network_event_test() -> Nil {
  let event =
    measurement.TelemetryEvent(
      name: [atom.create("pharos"), atom.create("host"), atom.create("network")],
      measurements: dict.from_list([
        #(atom.create("rx_bytes_per_sec"), dynamic.float(1000.0)),
        #(atom.create("tx_bytes_per_sec"), dynamic.float(500.0)),
        #(atom.create("rx_packets_per_sec"), dynamic.float(10.0)),
        #(atom.create("tx_packets_per_sec"), dynamic.float(5.0)),
      ]),
      metadata: dict.new(),
    )
  measurement.decode(event) |> should.equal(Ok(host_network()))
}

pub fn metric_of_host_network_test() -> Nil {
  let names =
    list.map(metric.from_measurement(host_network()), fn(m) { m.name })
  names
  |> should.equal([
    "host.network.rx_bytes_per_sec", "host.network.tx_bytes_per_sec",
    "host.network.rx_packets_per_sec", "host.network.tx_packets_per_sec",
  ])
}
