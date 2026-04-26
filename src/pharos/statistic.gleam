//// What pharos can poll, and how often.
////
//// A `Statistic` pairs a `StatisticKind` (the *what*) with a per-statistic
//// polling interval (the *how often*). Use `poll/1` to accept the kind's
//// default interval, or `poll_every/2` to override it.
////
//// Statistics group into these families:
////
//// - **BEAM VM internals** - `BeamMemory`, `BeamRunQueues`, `BeamSystemCounts`,
////   `BeamPersistentTerm`. Built into `telemetry_poller`. `BeamScheduler`
////   (scheduler utilisation) and `BeamReductions` are custom emitters.
//// - **Distributed Erlang topology** - `ClusterNodes`. Custom measurement
////   emitting `[pharos, cluster, nodes]`.
//// - **Per-process introspection** - `ProcessInfo`. Built into `telemetry_poller`.
//// - **Host (OS)** - `HostMemory` (`/proc/meminfo`, Linux), `HostDisk(path)`
////   and `HostCpu` (os_mon's `disksup`/`cpu_sup`), and `HostNetwork`
////   (`/proc/net/dev`). Host probes degrade to `status: unimplemented` when the
////   underlying source is unavailable.

import gleam/erlang/atom.{type Atom}

/// What kind of measurement to collect.
pub type StatisticKind {
  /// `[vm, memory]` - all keys from `erlang:memory/0`.
  BeamMemory
  /// `[vm, total_run_queue_lengths]` - total / cpu / io queue depths.
  BeamRunQueues
  /// `[vm, system_counts]` - process / atom / port counts and limits.
  BeamSystemCounts
  /// `[vm, persistent_term]` - persistent term count and memory usage.
  BeamPersistentTerm
  /// `[pharos, process, <name>]` - `process_info` snapshot for a named
  /// process. `event` overrides the emitted event name; `keys` selects which
  /// `process_info` keys to collect (e.g. `memory`, `message_queue_len`).
  ProcessInfo(name: Atom, event: List(Atom), keys: List(Atom))
  /// `[pharos, cluster, nodes]` - this node's name plus the list of
  /// connected distributed Erlang nodes.
  ClusterNodes
  /// `[pharos, host, memory]` - OS-level memory totals, read from
  /// `/proc/meminfo` (Linux).
  HostMemory
  /// `[pharos, host, disk]` - usage of the filesystem containing `path`, via
  /// os_mon's `disksup`.
  HostDisk(path: String)
  /// `[pharos, host, cpu]` - host CPU utilisation % and load averages, via
  /// os_mon's `cpu_sup`.
  HostCpu
  /// `[pharos, host, network]` - per-second network throughput summed across
  /// non-loopback interfaces, from `/proc/net/dev`.
  HostNetwork
  /// `[pharos, vm, scheduler]` - BEAM scheduler utilisation %, from
  /// `scheduler_wall_time`.
  BeamScheduler
  /// `[pharos, vm, reductions]` - BEAM reductions executed in the last
  /// interval.
  BeamReductions
}

/// A polling instruction: a kind to collect plus how often (in milliseconds).
pub type Statistic {
  Statistic(kind: StatisticKind, interval_ms: Int)
}

/// Poll `kind` at its default cadence.
pub fn poll(kind: StatisticKind) -> Statistic {
  Statistic(kind: kind, interval_ms: default_interval_ms(kind))
}

/// Poll `kind` every `interval_ms` milliseconds.
pub fn poll_every(kind: StatisticKind, interval_ms: Int) -> Statistic {
  Statistic(kind: kind, interval_ms: interval_ms)
}

/// Default polling cadence for each kind. Tuned to match the cost and
/// volatility of the underlying signal: cheap, fast-changing things
/// (run queues) poll often; expensive or slow-changing things (cluster
/// topology, disk) poll rarely.
pub fn default_interval_ms(kind: StatisticKind) -> Int {
  case kind {
    BeamMemory -> 5000
    BeamRunQueues -> 1000
    BeamSystemCounts -> 5000
    BeamPersistentTerm -> 10_000
    ProcessInfo(_, _, _) -> 5000
    ClusterNodes -> 30_000
    HostMemory -> 10_000
    HostDisk(_) -> 30_000
    HostCpu -> 5000
    HostNetwork -> 5000
    BeamScheduler -> 5000
    BeamReductions -> 5000
  }
}
