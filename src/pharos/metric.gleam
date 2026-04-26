//// The canonical metric record.
////
//// `Measurement` (BEAM/host) and `MetricSample` (custom probes) are the
//// *typed* shapes the collection and threshold lanes work with. `Metric` is
//// the *flat, transport-ready* shape the resilient pipeline buffers and ships:
//// a single named scalar with labels and a capture timestamp. One typed
//// measurement fans out into several `Metric`s (one per numeric field).
////
//// Keeping `Metric` deliberately flat (name + value + labels + timestamp)
//// means the hot buffer, spillover store, and sinks never need to know about
//// the BEAM/host/probe taxonomy: they move opaque scalars.

import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import pharos/internal/clock
import pharos/measurement.{
  type Measurement, BeamMemory, BeamPersistentTerm, BeamReductions,
  BeamRunQueues, BeamScheduler, BeamSystemCounts, ClusterNodes, HostCpu,
  HostDisk, HostMemory, HostNetwork, Implemented, ProcessInfo, Unimplemented,
}

/// A single named numeric reading at a point in time.
///
/// - `name` - dotted metric name, e.g. `"vm.memory.total"`.
/// - `value` - the reading, always a float (ints are widened on capture).
/// - `labels` - dimensions for the reading, e.g. `#("host", "node1")`.
/// - `timestamp_ms` - `erlang:system_time(millisecond)` at capture.
pub type Metric {
  Metric(
    name: String,
    value: Float,
    labels: Dict(String, String),
    timestamp_ms: Int,
  )
}

/// Build a metric with no labels, stamped with the current wall-clock time.
pub fn new(name name: String, value value: Float) -> Metric {
  Metric(
    name: name,
    value: value,
    labels: dict.new(),
    timestamp_ms: clock.now_ms(),
  )
}

/// Build a metric with labels, stamped with the current wall-clock time.
pub fn labelled(
  name name: String,
  value value: Float,
  labels labels: Dict(String, String),
) -> Metric {
  Metric(name: name, value: value, labels: labels, timestamp_ms: clock.now_ms())
}

/// Attach (or overwrite) a single label on a metric.
pub fn with_label(metric: Metric, key: String, value: String) -> Metric {
  Metric(..metric, labels: dict.insert(metric.labels, key, value))
}

// ---------------------------------------------------------------------------
// Flattening typed measurements into transport-ready metrics
// ---------------------------------------------------------------------------

/// Flatten a decoded `Measurement` into its constituent `Metric`s (one per
/// numeric field), all sharing a single capture timestamp. Non-numeric
/// measurements (`ProcessInfo`) and host probes still reporting
/// `Unimplemented` yield no metrics.
pub fn from_measurement(measurement: Measurement) -> List(Metric) {
  let ts = clock.now_ms()
  case measurement {
    BeamMemory(m) -> [
      at(ts, "vm.memory.total", m.total),
      at(ts, "vm.memory.processes", m.processes),
      at(ts, "vm.memory.system", m.system),
      at(ts, "vm.memory.binary", m.binary),
      at(ts, "vm.memory.ets", m.ets),
      at(ts, "vm.memory.atom", m.atom),
      at(ts, "vm.memory.code", m.code),
    ]
    BeamRunQueues(q) -> [
      at(ts, "vm.run_queue.total", int.to_float(q.total)),
      at(ts, "vm.run_queue.cpu", int.to_float(q.cpu)),
      at(ts, "vm.run_queue.io", int.to_float(q.io)),
    ]
    BeamSystemCounts(c) -> [
      at(ts, "vm.system.process_count", int.to_float(c.process_count)),
      at(ts, "vm.system.atom_count", int.to_float(c.atom_count)),
      at(ts, "vm.system.port_count", int.to_float(c.port_count)),
    ]
    BeamPersistentTerm(p) -> [
      at(ts, "vm.persistent_term.count", int.to_float(p.count)),
      at(ts, "vm.persistent_term.memory", p.memory),
    ]
    ClusterNodes(n) -> [
      at(ts, "cluster.nodes.count", int.to_float(list.length(n.nodes))),
    ]
    HostMemory(h) ->
      case h.status {
        Unimplemented -> []
        Implemented -> [
          at(ts, "host.memory.total", h.total),
          at(ts, "host.memory.used", h.used),
          at(ts, "host.memory.available", h.available),
        ]
      }
    HostDisk(d) ->
      case d.status {
        Unimplemented -> []
        Implemented -> [
          at(ts, "host.disk.total", d.total),
          at(ts, "host.disk.used", d.used),
          at(ts, "host.disk.available", d.available),
        ]
      }
    HostCpu(c) -> [
      at(ts, "host.cpu.util", c.util),
      at(ts, "host.cpu.load1", c.load1),
      at(ts, "host.cpu.load5", c.load5),
      at(ts, "host.cpu.load15", c.load15),
    ]
    HostNetwork(n) -> [
      at(ts, "host.network.rx_bytes_per_sec", n.rx_bytes_per_sec),
      at(ts, "host.network.tx_bytes_per_sec", n.tx_bytes_per_sec),
      at(ts, "host.network.rx_packets_per_sec", n.rx_packets_per_sec),
      at(ts, "host.network.tx_packets_per_sec", n.tx_packets_per_sec),
    ]
    BeamScheduler(s) -> [at(ts, "vm.scheduler.utilization", s.utilization)]
    BeamReductions(r) -> [
      at(ts, "vm.reductions.count", int.to_float(r.count)),
    ]
    ProcessInfo(_) -> []
  }
}

/// Flatten a custom probe `sample` (field -> value) into `Metric`s named
/// `probe.<probe_id>.<field>`.
pub fn from_sample(
  probe_id: String,
  sample: Dict(String, Float),
) -> List(Metric) {
  let ts = clock.now_ms()
  dict.to_list(sample)
  |> list.map(fn(pair) {
    let #(field, value) = pair
    at(ts, "probe." <> probe_id <> "." <> field, value)
  })
}

fn at(ts: Int, name: String, value: Float) -> Metric {
  Metric(name: name, value: value, labels: dict.new(), timestamp_ms: ts)
}
