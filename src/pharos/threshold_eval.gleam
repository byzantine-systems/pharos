//// Evaluate a `Threshold` against a decoded `Measurement`.
////
//// Returns a three-way verdict so callers can branch without reaching
//// for nested `case` over the cartesian product of measurements and
//// thresholds. Helper functions match every measurement variant
//// exhaustively, so adding a new measurement kind surfaces as a
//// compiler warning here.

import pharos/config.{
  type Threshold, AtomCount, BinaryMemory, CpuRunQueue, EtsMemory,
  PersistentTermCount, PersistentTermMemory, PortCount, ProcessCount,
  ProcessMemory, SystemMemory, TotalMemory, TotalRunQueue,
}
import pharos/measurement.{
  type BeamMemoryStats, type Measurement, type PersistentTermStats,
  type RunQueueStats, type SystemCountStats, BeamMemory, BeamPersistentTerm,
  BeamRunQueues, BeamSystemCounts, ClusterNodes, HostDisk, HostMemory,
  ProcessInfo,
}

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

/// Evaluate `threshold` against `measurement` and return a `Verdict`.
pub fn evaluate(measurement: Measurement, threshold: Threshold) -> Verdict {
  case threshold {
    TotalMemory(above: limit) ->
      check_memory(measurement, fn(memory) { memory.total >. limit })
    ProcessMemory(above: limit) ->
      check_memory(measurement, fn(memory) { memory.processes >. limit })
    SystemMemory(above: limit) ->
      check_memory(measurement, fn(memory) { memory.system >. limit })
    BinaryMemory(above: limit) ->
      check_memory(measurement, fn(memory) { memory.binary >. limit })
    EtsMemory(above: limit) ->
      check_memory(measurement, fn(memory) { memory.ets >. limit })

    TotalRunQueue(above: limit) ->
      check_run_queue(measurement, fn(queue) { queue.total > limit })
    CpuRunQueue(above: limit) ->
      check_run_queue(measurement, fn(queue) { queue.cpu > limit })

    ProcessCount(above: limit) ->
      check_system_counts(measurement, fn(counts) {
        counts.process_count > limit
      })
    AtomCount(above: limit) ->
      check_system_counts(measurement, fn(counts) { counts.atom_count > limit })
    PortCount(above: limit) ->
      check_system_counts(measurement, fn(counts) { counts.port_count > limit })

    PersistentTermCount(above: limit) ->
      check_persistent_term(measurement, fn(term) { term.count > limit })
    PersistentTermMemory(above: limit) ->
      check_persistent_term(measurement, fn(term) { term.memory >. limit })
  }
}

fn check_memory(
  measurement: Measurement,
  predicate: fn(BeamMemoryStats) -> Bool,
) -> Verdict {
  case measurement {
    BeamMemory(memory) -> verdict(predicate(memory))
    BeamRunQueues(_) -> NotApplicable
    BeamSystemCounts(_) -> NotApplicable
    BeamPersistentTerm(_) -> NotApplicable
    ProcessInfo(_) -> NotApplicable
    ClusterNodes(_) -> NotApplicable
    HostMemory(_) -> NotApplicable
    HostDisk(_) -> NotApplicable
  }
}

fn check_run_queue(
  measurement: Measurement,
  predicate: fn(RunQueueStats) -> Bool,
) -> Verdict {
  case measurement {
    BeamRunQueues(queue) -> verdict(predicate(queue))
    BeamMemory(_) -> NotApplicable
    BeamSystemCounts(_) -> NotApplicable
    BeamPersistentTerm(_) -> NotApplicable
    ProcessInfo(_) -> NotApplicable
    ClusterNodes(_) -> NotApplicable
    HostMemory(_) -> NotApplicable
    HostDisk(_) -> NotApplicable
  }
}

fn check_system_counts(
  measurement: Measurement,
  predicate: fn(SystemCountStats) -> Bool,
) -> Verdict {
  case measurement {
    BeamSystemCounts(counts) -> verdict(predicate(counts))
    BeamMemory(_) -> NotApplicable
    BeamRunQueues(_) -> NotApplicable
    BeamPersistentTerm(_) -> NotApplicable
    ProcessInfo(_) -> NotApplicable
    ClusterNodes(_) -> NotApplicable
    HostMemory(_) -> NotApplicable
    HostDisk(_) -> NotApplicable
  }
}

fn check_persistent_term(
  measurement: Measurement,
  predicate: fn(PersistentTermStats) -> Bool,
) -> Verdict {
  case measurement {
    BeamPersistentTerm(term) -> verdict(predicate(term))
    BeamMemory(_) -> NotApplicable
    BeamRunQueues(_) -> NotApplicable
    BeamSystemCounts(_) -> NotApplicable
    ProcessInfo(_) -> NotApplicable
    ClusterNodes(_) -> NotApplicable
    HostMemory(_) -> NotApplicable
    HostDisk(_) -> NotApplicable
  }
}

fn verdict(is_breached: Bool) -> Verdict {
  case is_breached {
    True -> Breached
    False -> Healthy
  }
}
