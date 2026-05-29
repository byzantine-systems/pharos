//// User-facing configuration for `pharos.start_link`.
////
//// Build a `Config` with `new/0` (sensible defaults), pipe through the
//// `with_*` setters to add statistics, thresholds, and tune the alert
//// timing knobs, then pass it to `pharos.start_link`.

import logging.{type LogLevel}
import pharos/alert.{type AlertLevel, Warning}
import pharos/measurement.{type MemoryUnit, Mb}
import pharos/probe.{type Probe, type ProbeThreshold}
import pharos/statistic.{type Statistic}

// ---------------------------------------------------------------------------
// Threshold types
//
// One variant per measurable dimension. Each variant maps 1-to-1 to a field
// in the corresponding record in pharos/measurement.gleam, so threshold
// evaluation is a straightforward pattern match with no string lookups.
//
// Memory thresholds are compared in the unit set by `Config.memory_unit`.
// ---------------------------------------------------------------------------

pub type Threshold {
  // BeamMemoryStats fields
  TotalMemory(above: Float)
  ProcessMemory(above: Float)
  SystemMemory(above: Float)
  BinaryMemory(above: Float)
  EtsMemory(above: Float)

  // RunQueueStats fields
  TotalRunQueue(above: Int)
  CpuRunQueue(above: Int)

  // SystemCountStats fields
  ProcessCount(above: Int)
  AtomCount(above: Int)
  PortCount(above: Int)

  // PersistentTermStats fields
  PersistentTermCount(above: Int)
  PersistentTermMemory(above: Float)
}

// ---------------------------------------------------------------------------
// Alert sink (where to route alert events)
// ---------------------------------------------------------------------------

/// Where pharos should route firing/resolved alert events. The `Log` sink
/// is wired up by default. `Process` and `Webhook` are placeholders for
/// future routing strategies (an OS message to a registered process, an
/// HTTP webhook).
pub type AlertSink {
  Log(level: LogLevel)
}

// ---------------------------------------------------------------------------
// Configuration schema
// ---------------------------------------------------------------------------

pub type Config {
  Config(
    /// Statistics to poll, each with its own interval.
    statistics: List(Statistic),
    /// Thresholds to evaluate on every matching measurement tick.
    /// An empty list means "observe only, never alert".
    thresholds: List(Threshold),
    /// Custom probes to poll alongside the built-in statistics. The pluggable
    /// lane for signals pharos has no built-in `StatisticKind` for.
    custom_statistics: List(Probe),
    /// Thresholds evaluated against custom probe samples.
    custom_thresholds: List(ProbeThreshold),
    /// Where to route firing/resolved alert events.
    alert_sinks: List(AlertSink),
    /// Unit for all memory threshold comparisons and decoded measurements.
    memory_unit: MemoryUnit,
    /// Milliseconds a threshold must stay breached before an alert fires.
    /// Set to `0` to fire immediately without a soak period.
    soak_period_ms: Int,
    /// Milliseconds recovery must hold before a firing alert resolves.
    cool_period_ms: Int,
    /// Default severity level attached to firing alerts.
    default_alert_level: AlertLevel,
  )
}

/// Sensible defaults: no statistics, no thresholds, log alerts at
/// `Warning`, scale memory in `Mb`, 30 s soak, 60 s cool, default alert
/// level `Warning`. Pipe through `with_*` setters to customise.
pub fn new() -> Config {
  Config(
    statistics: [],
    thresholds: [],
    custom_statistics: [],
    custom_thresholds: [],
    alert_sinks: [Log(level: logging.Warning)],
    memory_unit: Mb,
    soak_period_ms: 30_000,
    cool_period_ms: 60_000,
    default_alert_level: Warning,
  )
}

/// Set the statistics to poll. Replaces any previously configured list.
pub fn with_statistics(config: Config, statistics: List(Statistic)) -> Config {
  Config(..config, statistics: statistics)
}

/// Set the thresholds to evaluate. Replaces any previously configured list.
pub fn with_thresholds(config: Config, thresholds: List(Threshold)) -> Config {
  Config(..config, thresholds: thresholds)
}

/// Set the custom probes to poll. Replaces any previously configured list.
pub fn with_custom_statistics(config: Config, probes: List(Probe)) -> Config {
  Config(..config, custom_statistics: probes)
}

/// Set the custom probe thresholds. Replaces any previously configured list.
pub fn with_custom_thresholds(
  config: Config,
  thresholds: List(ProbeThreshold),
) -> Config {
  Config(..config, custom_thresholds: thresholds)
}

/// Set the alert sinks. Replaces any previously configured list.
pub fn with_alert_sinks(config: Config, sinks: List(AlertSink)) -> Config {
  Config(..config, alert_sinks: sinks)
}

/// Set the memory unit used for threshold comparisons and decoded
/// measurements.
pub fn with_memory_unit(config: Config, unit: MemoryUnit) -> Config {
  Config(..config, memory_unit: unit)
}

/// Set the soak period (milliseconds a threshold must stay breached before
/// firing). Set to 0 to fire immediately.
pub fn with_soak_period(config: Config, milliseconds: Int) -> Config {
  Config(..config, soak_period_ms: milliseconds)
}

/// Set the cool-down period (milliseconds recovery must hold before
/// resolving).
pub fn with_cool_period(config: Config, milliseconds: Int) -> Config {
  Config(..config, cool_period_ms: milliseconds)
}

/// Set the default alert level attached to firing alerts.
pub fn with_default_alert_level(config: Config, level: AlertLevel) -> Config {
  Config(..config, default_alert_level: level)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Stable string id derived from a threshold variant. Used to register
/// per-threshold alert managers under a deterministic name so they can be
/// looked up after a restart.
pub fn threshold_id(threshold: Threshold) -> String {
  case threshold {
    TotalMemory(_) -> "total_memory"
    ProcessMemory(_) -> "process_memory"
    SystemMemory(_) -> "system_memory"
    BinaryMemory(_) -> "binary_memory"
    EtsMemory(_) -> "ets_memory"
    TotalRunQueue(_) -> "total_run_queue"
    CpuRunQueue(_) -> "cpu_run_queue"
    ProcessCount(_) -> "process_count"
    AtomCount(_) -> "atom_count"
    PortCount(_) -> "port_count"
    PersistentTermCount(_) -> "persistent_term_count"
    PersistentTermMemory(_) -> "persistent_term_memory"
  }
}
