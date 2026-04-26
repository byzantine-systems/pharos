//// Decoded measurement values.
////
//// `telemetry_poller` and pharos's custom emitters dispatch raw telemetry
//// events; `decode/2` lifts them into typed `Measurement` records. Memory
//// values arrive in bytes and are scaled to the configured `MemoryUnit`.

import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/erlang/atom.{type Atom}
import gleam/int
import gleam/list
import gleam/result

// ---------------------------------------------------------------------------
// Memory unit
// ---------------------------------------------------------------------------

/// Unit to express BEAM memory values in. Raw `erlang:memory/0` returns
/// bytes; `decode_with_unit/2` scales them to the requested unit.
pub type MemoryUnit {
  Kb
  Mb
  Gb
}

// ---------------------------------------------------------------------------
// Telemetry event (input to decode)
// ---------------------------------------------------------------------------

/// A telemetry event as dispatched by `telemetry:execute/3`.
pub type TelemetryEvent {
  TelemetryEvent(
    name: List(Atom),
    measurements: Dict(Atom, Dynamic),
    metadata: Dict(Atom, Dynamic),
  )
}

// ---------------------------------------------------------------------------
// Per-kind measurement records
// ---------------------------------------------------------------------------

pub type BeamMemoryStats {
  BeamMemoryStats(
    unit: MemoryUnit,
    total: Float,
    processes: Float,
    processes_used: Float,
    system: Float,
    atom: Float,
    atom_used: Float,
    binary: Float,
    code: Float,
    ets: Float,
  )
}

pub type RunQueueStats {
  RunQueueStats(total: Int, cpu: Int, io: Int)
}

pub type SystemCountStats {
  SystemCountStats(
    process_count: Int,
    atom_count: Int,
    port_count: Int,
    process_limit: Int,
    atom_limit: Int,
    port_limit: Int,
  )
}

pub type PersistentTermStats {
  PersistentTermStats(count: Int, unit: MemoryUnit, memory: Float)
}

pub type ProcessInfoStats {
  ProcessInfoStats(name: Atom, info: Dict(Atom, Dynamic))
}

pub type ClusterNodeStats {
  ClusterNodeStats(self: String, nodes: List(String))
}

/// Status of a scaffolded host probe: `Implemented` once a real probe lands,
/// `Unimplemented` while the host module is a placeholder.
pub type HostProbeStatus {
  Implemented
  Unimplemented
}

pub type HostMemoryStats {
  HostMemoryStats(
    status: HostProbeStatus,
    unit: MemoryUnit,
    total: Float,
    used: Float,
    available: Float,
  )
}

pub type HostDiskStats {
  HostDiskStats(
    status: HostProbeStatus,
    unit: MemoryUnit,
    total: Float,
    used: Float,
    available: Float,
  )
}

/// OS-level CPU stats: `util` is busy percentage since the previous sample
/// (0-100); `load1`/`load5`/`load15` are the 1/5/15-minute load averages.
pub type HostCpuStats {
  HostCpuStats(util: Float, load1: Float, load5: Float, load15: Float)
}

/// OS-level network throughput, summed across non-loopback interfaces and
/// expressed as per-second rates derived from `/proc/net/dev` counters.
pub type HostNetworkStats {
  HostNetworkStats(
    rx_bytes_per_sec: Float,
    tx_bytes_per_sec: Float,
    rx_packets_per_sec: Float,
    tx_packets_per_sec: Float,
  )
}

/// BEAM scheduler utilisation: busy percentage (0-100) across all schedulers
/// since the previous sample, from `erlang:statistics(scheduler_wall_time)`.
pub type SchedulerStats {
  SchedulerStats(utilization: Float)
}

/// BEAM reduction count executed during the last polling interval, from the
/// "since last call" element of `erlang:statistics(reductions)`.
pub type ReductionStats {
  ReductionStats(count: Int)
}

// ---------------------------------------------------------------------------
// Discriminated union
// ---------------------------------------------------------------------------

pub type Measurement {
  BeamMemory(BeamMemoryStats)
  BeamRunQueues(RunQueueStats)
  BeamSystemCounts(SystemCountStats)
  BeamPersistentTerm(PersistentTermStats)
  ProcessInfo(ProcessInfoStats)
  ClusterNodes(ClusterNodeStats)
  HostMemory(HostMemoryStats)
  HostDisk(HostDiskStats)
  HostCpu(HostCpuStats)
  HostNetwork(HostNetworkStats)
  BeamScheduler(SchedulerStats)
  BeamReductions(ReductionStats)
}

// ---------------------------------------------------------------------------
// Public decode API
// ---------------------------------------------------------------------------

/// Decode a `TelemetryEvent` into a typed `Measurement`, scaling any memory
/// values to `Mb` by default.
pub fn decode(event: TelemetryEvent) -> Result(Measurement, String) {
  decode_with_unit(event, Mb)
}

/// Decode a `TelemetryEvent` into a typed `Measurement`, scaling any memory
/// values to `unit`.
pub fn decode_with_unit(
  event: TelemetryEvent,
  unit: MemoryUnit,
) -> Result(Measurement, String) {
  case list.map(event.name, atom.to_string) {
    ["vm", "memory"] ->
      decode_beam_memory(event.measurements, unit)
      |> result.map(BeamMemory)

    ["vm", "total_run_queue_lengths"] ->
      decode_run_queues(event.measurements)
      |> result.map(BeamRunQueues)

    ["vm", "system_counts"] ->
      decode_system_counts(event.measurements)
      |> result.map(BeamSystemCounts)

    ["vm", "persistent_term"] ->
      decode_persistent_term(event.measurements, unit)
      |> result.map(BeamPersistentTerm)

    ["pharos", "cluster", "nodes"] ->
      decode_cluster_nodes(event.metadata)
      |> result.map(ClusterNodes)

    ["pharos", "host", "memory"] ->
      decode_host_memory(event.measurements, event.metadata, unit)
      |> result.map(HostMemory)

    ["pharos", "host", "disk"] ->
      decode_host_disk(event.measurements, event.metadata, unit)
      |> result.map(HostDisk)

    ["pharos", "host", "cpu"] ->
      decode_host_cpu(event.measurements)
      |> result.map(HostCpu)

    ["pharos", "host", "network"] ->
      decode_host_network(event.measurements)
      |> result.map(HostNetwork)

    ["pharos", "vm", "scheduler"] ->
      decode_scheduler(event.measurements)
      |> result.map(BeamScheduler)

    ["pharos", "vm", "reductions"] ->
      decode_reductions(event.measurements)
      |> result.map(BeamReductions)

    _other ->
      decode_process_info(event.measurements, event.metadata)
      |> result.map(ProcessInfo)
  }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn get_int(
  measurements: Dict(Atom, Dynamic),
  key: String,
) -> Result(Int, String) {
  case dict.get(measurements, atom.create(key)) {
    Error(Nil) -> Error("missing key: " <> key)
    Ok(dynamic_value) ->
      decode.run(dynamic_value, decode.int)
      |> result.map_error(fn(_) { "expected int for key: " <> key })
  }
}

/// Decode a numeric measurement as a `Float`, accepting an integer (widened)
/// or a float. Used for rates / percentages that may arrive as either.
fn get_number(
  measurements: Dict(Atom, Dynamic),
  key: String,
) -> Result(Float, String) {
  case dict.get(measurements, atom.create(key)) {
    Error(Nil) -> Error("missing key: " <> key)
    Ok(dynamic_value) ->
      decode.run(
        dynamic_value,
        decode.one_of(decode.float, [decode.int |> decode.map(int.to_float)]),
      )
      |> result.map_error(fn(_) { "expected number for key: " <> key })
  }
}

fn to_unit(bytes: Int, unit: MemoryUnit) -> Float {
  let as_float = int.to_float(bytes)
  case unit {
    Kb -> as_float /. 1024.0
    Mb -> as_float /. 1_048_576.0
    Gb -> as_float /. 1_073_741_824.0
  }
}

fn get_bytes(
  measurements: Dict(Atom, Dynamic),
  key: String,
  unit: MemoryUnit,
) -> Result(Float, String) {
  use bytes <- result.try(get_int(measurements, key))
  Ok(to_unit(bytes, unit))
}

fn decode_beam_memory(
  measurements: Dict(Atom, Dynamic),
  unit: MemoryUnit,
) -> Result(BeamMemoryStats, String) {
  use total <- result.try(get_bytes(measurements, "total", unit))
  use processes <- result.try(get_bytes(measurements, "processes", unit))
  use processes_used <- result.try(get_bytes(
    measurements,
    "processes_used",
    unit,
  ))
  use system <- result.try(get_bytes(measurements, "system", unit))
  use atom_value <- result.try(get_bytes(measurements, "atom", unit))
  use atom_used <- result.try(get_bytes(measurements, "atom_used", unit))
  use binary <- result.try(get_bytes(measurements, "binary", unit))
  use code <- result.try(get_bytes(measurements, "code", unit))
  use ets <- result.try(get_bytes(measurements, "ets", unit))
  Ok(BeamMemoryStats(
    unit: unit,
    total: total,
    processes: processes,
    processes_used: processes_used,
    system: system,
    atom: atom_value,
    atom_used: atom_used,
    binary: binary,
    code: code,
    ets: ets,
  ))
}

fn decode_run_queues(
  measurements: Dict(Atom, Dynamic),
) -> Result(RunQueueStats, String) {
  use total <- result.try(get_int(measurements, "total"))
  use cpu <- result.try(get_int(measurements, "cpu"))
  use io <- result.try(get_int(measurements, "io"))
  Ok(RunQueueStats(total: total, cpu: cpu, io: io))
}

fn decode_system_counts(
  measurements: Dict(Atom, Dynamic),
) -> Result(SystemCountStats, String) {
  use process_count <- result.try(get_int(measurements, "process_count"))
  use atom_count <- result.try(get_int(measurements, "atom_count"))
  use port_count <- result.try(get_int(measurements, "port_count"))
  use process_limit <- result.try(get_int(measurements, "process_limit"))
  use atom_limit <- result.try(get_int(measurements, "atom_limit"))
  use port_limit <- result.try(get_int(measurements, "port_limit"))
  Ok(SystemCountStats(
    process_count: process_count,
    atom_count: atom_count,
    port_count: port_count,
    process_limit: process_limit,
    atom_limit: atom_limit,
    port_limit: port_limit,
  ))
}

fn decode_persistent_term(
  measurements: Dict(Atom, Dynamic),
  unit: MemoryUnit,
) -> Result(PersistentTermStats, String) {
  use count <- result.try(get_int(measurements, "count"))
  use memory <- result.try(get_bytes(measurements, "memory", unit))
  Ok(PersistentTermStats(count: count, unit: unit, memory: memory))
}

fn decode_process_info(
  measurements: Dict(Atom, Dynamic),
  metadata: Dict(Atom, Dynamic),
) -> Result(ProcessInfoStats, String) {
  case dict.get(metadata, atom.create("name")) {
    Error(Nil) -> Error("process_info event missing 'name' in metadata")
    Ok(dynamic_value) ->
      decode.run(dynamic_value, atom.decoder())
      |> result.map(fn(name) {
        ProcessInfoStats(name: name, info: measurements)
      })
      |> result.map_error(fn(_) {
        "expected atom for 'name' in process_info metadata"
      })
  }
}

fn decode_cluster_nodes(
  metadata: Dict(Atom, Dynamic),
) -> Result(ClusterNodeStats, String) {
  use self <- result.try(get_string(metadata, "self"))
  use nodes <- result.try(get_string_list(metadata, "nodes"))
  Ok(ClusterNodeStats(self: self, nodes: nodes))
}

fn decode_host_memory(
  measurements: Dict(Atom, Dynamic),
  metadata: Dict(Atom, Dynamic),
  unit: MemoryUnit,
) -> Result(HostMemoryStats, String) {
  let status = decode_status(metadata)
  use total <- result.try(get_bytes(measurements, "total_bytes", unit))
  use used <- result.try(get_bytes(measurements, "used_bytes", unit))
  use available <- result.try(get_bytes(measurements, "available_bytes", unit))
  Ok(HostMemoryStats(
    status: status,
    unit: unit,
    total: total,
    used: used,
    available: available,
  ))
}

fn decode_host_disk(
  measurements: Dict(Atom, Dynamic),
  metadata: Dict(Atom, Dynamic),
  unit: MemoryUnit,
) -> Result(HostDiskStats, String) {
  let status = decode_status(metadata)
  use total <- result.try(get_bytes(measurements, "total_bytes", unit))
  use used <- result.try(get_bytes(measurements, "used_bytes", unit))
  use available <- result.try(get_bytes(measurements, "available_bytes", unit))
  Ok(HostDiskStats(
    status: status,
    unit: unit,
    total: total,
    used: used,
    available: available,
  ))
}

fn decode_host_cpu(
  measurements: Dict(Atom, Dynamic),
) -> Result(HostCpuStats, String) {
  use util <- result.try(get_number(measurements, "util"))
  use load1 <- result.try(get_number(measurements, "load1"))
  use load5 <- result.try(get_number(measurements, "load5"))
  use load15 <- result.try(get_number(measurements, "load15"))
  Ok(HostCpuStats(util: util, load1: load1, load5: load5, load15: load15))
}

fn decode_host_network(
  measurements: Dict(Atom, Dynamic),
) -> Result(HostNetworkStats, String) {
  use rx_bytes <- result.try(get_number(measurements, "rx_bytes_per_sec"))
  use tx_bytes <- result.try(get_number(measurements, "tx_bytes_per_sec"))
  use rx_packets <- result.try(get_number(measurements, "rx_packets_per_sec"))
  use tx_packets <- result.try(get_number(measurements, "tx_packets_per_sec"))
  Ok(HostNetworkStats(
    rx_bytes_per_sec: rx_bytes,
    tx_bytes_per_sec: tx_bytes,
    rx_packets_per_sec: rx_packets,
    tx_packets_per_sec: tx_packets,
  ))
}

fn decode_scheduler(
  measurements: Dict(Atom, Dynamic),
) -> Result(SchedulerStats, String) {
  use utilization <- result.try(get_number(measurements, "utilization"))
  Ok(SchedulerStats(utilization: utilization))
}

fn decode_reductions(
  measurements: Dict(Atom, Dynamic),
) -> Result(ReductionStats, String) {
  use count <- result.try(get_int(measurements, "count"))
  Ok(ReductionStats(count: count))
}

fn decode_status(metadata: Dict(Atom, Dynamic)) -> HostProbeStatus {
  case dict.get(metadata, atom.create("status")) {
    Ok(dynamic_value) ->
      case decode.run(dynamic_value, atom.decoder()) {
        Ok(value) ->
          case atom.to_string(value) {
            "unimplemented" -> Unimplemented
            _other -> Implemented
          }
        Error(_) -> Implemented
      }
    Error(Nil) -> Implemented
  }
}

fn get_string(
  values: Dict(Atom, Dynamic),
  key: String,
) -> Result(String, String) {
  case dict.get(values, atom.create(key)) {
    Error(Nil) -> Error("missing key: " <> key)
    Ok(dynamic_value) ->
      decode.run(dynamic_value, decode.string)
      |> result.map_error(fn(_) { "expected string for key: " <> key })
  }
}

fn get_string_list(
  values: Dict(Atom, Dynamic),
  key: String,
) -> Result(List(String), String) {
  case dict.get(values, atom.create(key)) {
    Error(Nil) -> Error("missing key: " <> key)
    Ok(dynamic_value) ->
      decode.run(dynamic_value, decode.list(decode.string))
      |> result.map_error(fn(_) { "expected list of strings for key: " <> key })
  }
}
