//// Evaluate a `Threshold` against a decoded `Measurement`.
////
//// Returns a three-way verdict so callers can branch without reaching
//// for nested `case` over the cartesian product of measurements and
//// thresholds. Helper functions match every measurement variant
//// exhaustively, so adding a new measurement kind surfaces as a
//// compiler warning here.

import gleam/dict
import gleam/int
import gleam/list
import gleam/result
import pharos/config
import pharos/measurement
import pharos/metric
import pharos/probe

/// Three-way verdict for a (measurement, threshold) pair.
pub type Verdict {
  /// The threshold doesn't apply to this measurement kind. Caller should
  /// do nothing (don't fire `recover` for unrelated measurements).
  NotApplicable
  /// The threshold applies and is currently within limits.
  Healthy
  /// The threshold applies and is currently breached.
  Breached
}

/// Evaluate a probe `threshold` against a decoded probe `sample`, returning
/// the same `Verdict` as `evaluate/2`. A `NotApplicable` verdict means the
/// threshold's field is absent from this sample, so the caller should neither
/// breach nor recover.
pub fn evaluate_probe(
  sample: probe.MetricSample,
  threshold: probe.ProbeThreshold,
) -> Verdict {
  case dict.get(sample, threshold.field) {
    Error(Nil) -> NotApplicable
    Ok(value) ->
      case threshold.comparison {
        probe.Above(limit) -> verdict(value >. limit)
        probe.Below(limit) -> verdict(value <. limit)
      }
  }
}

/// Evaluate `threshold` against `measurement` and return a `Verdict`.
pub fn evaluate(
  measurement: measurement.Measurement,
  threshold: config.Threshold,
) -> Verdict {
  case threshold {
    config.TotalMemory(above: limit) ->
      check_memory(measurement, fn(memory) { memory.total >. limit })
    config.ProcessMemory(above: limit) ->
      check_memory(measurement, fn(memory) { memory.processes >. limit })
    config.SystemMemory(above: limit) ->
      check_memory(measurement, fn(memory) { memory.system >. limit })
    config.BinaryMemory(above: limit) ->
      check_memory(measurement, fn(memory) { memory.binary >. limit })
    config.EtsMemory(above: limit) ->
      check_memory(measurement, fn(memory) { memory.ets >. limit })

    config.TotalRunQueue(above: limit) ->
      check_run_queue(measurement, fn(queue) { queue.total > limit })
    config.CpuRunQueue(above: limit) ->
      check_run_queue(measurement, fn(queue) { queue.cpu > limit })

    config.ProcessCount(above: limit) ->
      check_system_counts(measurement, fn(counts) {
        counts.process_count > limit
      })
    config.AtomCount(above: limit) ->
      check_system_counts(measurement, fn(counts) { counts.atom_count > limit })
    config.PortCount(above: limit) ->
      check_system_counts(measurement, fn(counts) { counts.port_count > limit })

    config.PersistentTermCount(above: limit) ->
      check_persistent_term(measurement, fn(term) { term.count > limit })
    config.PersistentTermMemory(above: limit) ->
      check_persistent_term(measurement, fn(term) { term.memory >. limit })

    config.HostMemoryUsed(above: limit) ->
      check_host_memory(measurement, fn(host) { host.used >. limit })
    config.HostDiskUsed(above: limit) ->
      check_host_disk(measurement, fn(disk) { disk.used >. limit })
    config.HostCpuUtil(above: limit) ->
      check_host_cpu(measurement, fn(cpu) { cpu.util >. limit })
    config.SchedulerUtilization(above: limit) ->
      check_scheduler(measurement, fn(sched) { sched.utilization >. limit })

    // Instantaneous fallback: windowed semantics are applied in the routing /
    // alert_manager path (per-sample via `sample_value`), not here.
    config.Windowed(over:, ..) -> evaluate(measurement, over)
  }
}

// ---------------------------------------------------------------------------
// Sliding-window support
// ---------------------------------------------------------------------------

/// Extract the current value of the metric `threshold` compares, by name, from
/// the measurement's flattened metrics. Returns `Error(Nil)` when the
/// measurement doesn't carry that metric (wrong kind, or an `Unimplemented`
/// host probe, which flattens to no metrics), so such ticks contribute no
/// sample to a window.
pub fn sample_value(
  measurement: measurement.Measurement,
  threshold: config.Threshold,
) -> Result(Float, Nil) {
  let name = metric_name(threshold)
  metric.from_measurement(measurement)
  |> list.find(fn(m) { m.name == name })
  |> result.map(fn(m) { m.value })
}

/// The flattened metric name a threshold compares against (see
/// `metric.from_measurement`).
pub fn metric_name(threshold: config.Threshold) -> String {
  case threshold {
    config.TotalMemory(_) -> "vm.memory.total"
    config.ProcessMemory(_) -> "vm.memory.processes"
    config.SystemMemory(_) -> "vm.memory.system"
    config.BinaryMemory(_) -> "vm.memory.binary"
    config.EtsMemory(_) -> "vm.memory.ets"
    config.TotalRunQueue(_) -> "vm.run_queue.total"
    config.CpuRunQueue(_) -> "vm.run_queue.cpu"
    config.ProcessCount(_) -> "vm.system.process_count"
    config.AtomCount(_) -> "vm.system.atom_count"
    config.PortCount(_) -> "vm.system.port_count"
    config.PersistentTermCount(_) -> "vm.persistent_term.count"
    config.PersistentTermMemory(_) -> "vm.persistent_term.memory"
    config.HostMemoryUsed(_) -> "host.memory.used"
    config.HostDiskUsed(_) -> "host.disk.used"
    config.HostCpuUtil(_) -> "host.cpu.util"
    config.SchedulerUtilization(_) -> "vm.scheduler.utilization"
    config.Windowed(over:, ..) -> metric_name(over)
  }
}

/// The threshold's comparison limit as a `Float` (integer limits are widened).
pub fn threshold_limit(threshold: config.Threshold) -> Float {
  case threshold {
    config.TotalMemory(above:) -> above
    config.ProcessMemory(above:) -> above
    config.SystemMemory(above:) -> above
    config.BinaryMemory(above:) -> above
    config.EtsMemory(above:) -> above
    config.PersistentTermMemory(above:) -> above
    config.HostMemoryUsed(above:) -> above
    config.HostDiskUsed(above:) -> above
    config.HostCpuUtil(above:) -> above
    config.SchedulerUtilization(above:) -> above
    config.TotalRunQueue(above:) -> int.to_float(above)
    config.CpuRunQueue(above:) -> int.to_float(above)
    config.ProcessCount(above:) -> int.to_float(above)
    config.AtomCount(above:) -> int.to_float(above)
    config.PortCount(above:) -> int.to_float(above)
    config.PersistentTermCount(above:) -> int.to_float(above)
    config.Windowed(over:, ..) -> threshold_limit(over)
  }
}

fn check_memory(
  measurement: measurement.Measurement,
  predicate: fn(measurement.BeamMemoryStats) -> Bool,
) -> Verdict {
  case measurement {
    measurement.BeamMemory(memory) -> verdict(predicate(memory))
    measurement.BeamRunQueues(_) -> NotApplicable
    measurement.BeamSystemCounts(_) -> NotApplicable
    measurement.BeamPersistentTerm(_) -> NotApplicable
    measurement.ProcessInfo(_) -> NotApplicable
    measurement.ClusterNodes(_) -> NotApplicable
    measurement.HostMemory(_) -> NotApplicable
    measurement.HostDisk(_) -> NotApplicable
    measurement.HostCpu(_) -> NotApplicable
    measurement.HostNetwork(_) -> NotApplicable
    measurement.BeamScheduler(_) -> NotApplicable
    measurement.BeamReductions(_) -> NotApplicable
  }
}

fn check_run_queue(
  measurement: measurement.Measurement,
  predicate: fn(measurement.RunQueueStats) -> Bool,
) -> Verdict {
  case measurement {
    measurement.BeamRunQueues(queue) -> verdict(predicate(queue))
    measurement.BeamMemory(_) -> NotApplicable
    measurement.BeamSystemCounts(_) -> NotApplicable
    measurement.BeamPersistentTerm(_) -> NotApplicable
    measurement.ProcessInfo(_) -> NotApplicable
    measurement.ClusterNodes(_) -> NotApplicable
    measurement.HostMemory(_) -> NotApplicable
    measurement.HostDisk(_) -> NotApplicable
    measurement.HostCpu(_) -> NotApplicable
    measurement.HostNetwork(_) -> NotApplicable
    measurement.BeamScheduler(_) -> NotApplicable
    measurement.BeamReductions(_) -> NotApplicable
  }
}

fn check_system_counts(
  measurement: measurement.Measurement,
  predicate: fn(measurement.SystemCountStats) -> Bool,
) -> Verdict {
  case measurement {
    measurement.BeamSystemCounts(counts) -> verdict(predicate(counts))
    measurement.BeamMemory(_) -> NotApplicable
    measurement.BeamRunQueues(_) -> NotApplicable
    measurement.BeamPersistentTerm(_) -> NotApplicable
    measurement.ProcessInfo(_) -> NotApplicable
    measurement.ClusterNodes(_) -> NotApplicable
    measurement.HostMemory(_) -> NotApplicable
    measurement.HostDisk(_) -> NotApplicable
    measurement.HostCpu(_) -> NotApplicable
    measurement.HostNetwork(_) -> NotApplicable
    measurement.BeamScheduler(_) -> NotApplicable
    measurement.BeamReductions(_) -> NotApplicable
  }
}

fn check_persistent_term(
  measurement: measurement.Measurement,
  predicate: fn(measurement.PersistentTermStats) -> Bool,
) -> Verdict {
  case measurement {
    measurement.BeamPersistentTerm(term) -> verdict(predicate(term))
    measurement.BeamMemory(_) -> NotApplicable
    measurement.BeamRunQueues(_) -> NotApplicable
    measurement.BeamSystemCounts(_) -> NotApplicable
    measurement.ProcessInfo(_) -> NotApplicable
    measurement.ClusterNodes(_) -> NotApplicable
    measurement.HostMemory(_) -> NotApplicable
    measurement.HostDisk(_) -> NotApplicable
    measurement.HostCpu(_) -> NotApplicable
    measurement.HostNetwork(_) -> NotApplicable
    measurement.BeamScheduler(_) -> NotApplicable
    measurement.BeamReductions(_) -> NotApplicable
  }
}

/// Evaluate a host-memory threshold. A host probe still reporting
/// `Unimplemented` (e.g. the non-Linux `/proc/meminfo` fallback) is treated as
/// `NotApplicable` so its placeholder zeros never produce breach/recover noise.
fn check_host_memory(
  measurement: measurement.Measurement,
  predicate: fn(measurement.HostMemoryStats) -> Bool,
) -> Verdict {
  case measurement {
    measurement.HostMemory(host) ->
      check_host_probe(host.status, host, predicate)
    measurement.BeamMemory(_) -> NotApplicable
    measurement.BeamRunQueues(_) -> NotApplicable
    measurement.BeamSystemCounts(_) -> NotApplicable
    measurement.BeamPersistentTerm(_) -> NotApplicable
    measurement.ProcessInfo(_) -> NotApplicable
    measurement.ClusterNodes(_) -> NotApplicable
    measurement.HostDisk(_) -> NotApplicable
    measurement.HostCpu(_) -> NotApplicable
    measurement.HostNetwork(_) -> NotApplicable
    measurement.BeamScheduler(_) -> NotApplicable
    measurement.BeamReductions(_) -> NotApplicable
  }
}

/// Evaluate a host-disk threshold. Like host memory, a probe still reporting
/// `Unimplemented` (os_mon unavailable) is `NotApplicable`.
fn check_host_disk(
  measurement: measurement.Measurement,
  predicate: fn(measurement.HostDiskStats) -> Bool,
) -> Verdict {
  case measurement {
    measurement.HostDisk(disk) -> check_host_probe(disk.status, disk, predicate)
    measurement.BeamMemory(_) -> NotApplicable
    measurement.BeamRunQueues(_) -> NotApplicable
    measurement.BeamSystemCounts(_) -> NotApplicable
    measurement.BeamPersistentTerm(_) -> NotApplicable
    measurement.ProcessInfo(_) -> NotApplicable
    measurement.ClusterNodes(_) -> NotApplicable
    measurement.HostMemory(_) -> NotApplicable
    measurement.HostCpu(_) -> NotApplicable
    measurement.HostNetwork(_) -> NotApplicable
    measurement.BeamScheduler(_) -> NotApplicable
    measurement.BeamReductions(_) -> NotApplicable
  }
}

fn check_host_cpu(
  measurement: measurement.Measurement,
  predicate: fn(measurement.HostCpuStats) -> Bool,
) -> Verdict {
  case measurement {
    measurement.HostCpu(cpu) -> verdict(predicate(cpu))
    measurement.BeamMemory(_) -> NotApplicable
    measurement.BeamRunQueues(_) -> NotApplicable
    measurement.BeamSystemCounts(_) -> NotApplicable
    measurement.BeamPersistentTerm(_) -> NotApplicable
    measurement.ProcessInfo(_) -> NotApplicable
    measurement.ClusterNodes(_) -> NotApplicable
    measurement.HostMemory(_) -> NotApplicable
    measurement.HostDisk(_) -> NotApplicable
    measurement.HostNetwork(_) -> NotApplicable
    measurement.BeamScheduler(_) -> NotApplicable
    measurement.BeamReductions(_) -> NotApplicable
  }
}

fn check_scheduler(
  measurement: measurement.Measurement,
  predicate: fn(measurement.SchedulerStats) -> Bool,
) -> Verdict {
  case measurement {
    measurement.BeamScheduler(sched) -> verdict(predicate(sched))
    measurement.BeamMemory(_) -> NotApplicable
    measurement.BeamRunQueues(_) -> NotApplicable
    measurement.BeamSystemCounts(_) -> NotApplicable
    measurement.BeamPersistentTerm(_) -> NotApplicable
    measurement.ProcessInfo(_) -> NotApplicable
    measurement.ClusterNodes(_) -> NotApplicable
    measurement.HostMemory(_) -> NotApplicable
    measurement.HostDisk(_) -> NotApplicable
    measurement.HostCpu(_) -> NotApplicable
    measurement.HostNetwork(_) -> NotApplicable
    measurement.BeamReductions(_) -> NotApplicable
  }
}

/// Apply `predicate` to a host probe's stats unless it is still reporting
/// `Unimplemented` (its source unavailable), in which case it is
/// `NotApplicable` so placeholder values never breach/recover.
fn check_host_probe(
  status: measurement.HostProbeStatus,
  stats: stats,
  predicate: fn(stats) -> Bool,
) -> Verdict {
  case status {
    measurement.Unimplemented -> NotApplicable
    measurement.Implemented -> verdict(predicate(stats))
  }
}

fn verdict(is_breached: Bool) -> Verdict {
  case is_breached {
    True -> Breached
    False -> Healthy
  }
}
