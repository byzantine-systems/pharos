//// User-facing configuration for `pharos.start_link`.
////
//// Build a `Config` with `new/0` (sensible defaults), pipe through the
//// `with_*` setters to add statistics, thresholds, and tune the alert
//// timing knobs, then pass it to `pharos.start_link`.

import gleam/float
import gleam/option.{type Option, None, Some}
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

  // HostMemoryStats fields
  HostMemoryUsed(above: Float)

  // HostDiskStats / HostCpuStats / SchedulerStats fields
  HostDiskUsed(above: Float)
  HostCpuUtil(above: Float)
  SchedulerUtilization(above: Float)

  /// Sliding-window wrapper: evaluates `over` metrics aggregated across the
  /// last `window_ms` of samples per `mode`, instead of per-tick. `over`
  /// supplies the metric and limit, nesting `Windowed` is not supported.
  Windowed(over: Threshold, window_ms: Int, mode: alert.WindowMode)
}

// ---------------------------------------------------------------------------
// Alert sink (where to route alert events)
// ---------------------------------------------------------------------------

/// Where pharos should route firing/resolved alert events. Each configured
/// sink becomes one handler on the alert event bus (see `pharos/sink`).
///
/// - `Console` logs the alert locally (NixOS `journalctl` debugging).
/// - `Webhook` POSTs a JSON body to `url` (e.g. Slack) for direct notification
///   when the Brain is unreachable.
/// - `Brain` ETF-encodes the alert and sends it to the registered process
///   `name` on `node` for native BEAM-to-BEAM delivery (an empty `node`
///   targets the local node).
/// - `Otlp` exports to an OpenTelemetry collector at `endpoint`. Currently a
///   documented stub pending the Protobuf/gRPC exporter.
pub type AlertSink {
  Console(level: LogLevel)
  Webhook(url: String)
  Brain(node: String, name: String)
  Otlp(endpoint: String)
}

// ---------------------------------------------------------------------------
// Metric stream (the resilient pipeline to the Brain)
// ---------------------------------------------------------------------------

/// Target for the buffered metric stream managed by the connection manager.
/// `node` is the remote BEAM node (empty string = local node); `name` is the
/// registered process to deliver ETF payloads to.
pub type BrainStream {
  BrainStream(node: String, name: String)
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
    /// Capacity of the in-memory hot buffer (number of recent metrics kept
    /// before the oldest are evicted).
    metric_buffer_capacity: Int,
    /// Where to stream buffered metrics. `None` disables the connection
    /// manager (metrics are still buffered locally, just not shipped).
    brain: Option(BrainStream),
    /// On-disk path for the Dets cold-tier spillover file. `None` keeps metrics
    /// RAM-only. Only effective when a Brain stream is also configured: with no
    /// connection there is nothing to spill for. When set, metrics spill to
    /// disk if the connection is lost and the hot buffer fills, and are
    /// replayed (disk first) on reconnect.
    metric_spillover_path: Option(String),
    /// Polling jitter as a fraction of each statistic's interval, in `[0.0, 1.0]`.
    /// When `Some(ratio)`, each poller starts after a random delay in
    /// `[0, interval * ratio)`, permanently phase-shifting its periodic schedule
    /// so a fleet of agents never polls (and ships to the Brain) in lockstep.
    /// `None` (the default) disables jitter and leaves polling unchanged.
    poll_jitter: Option(Float),
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
    alert_sinks: [Console(level: logging.Warning)],
    memory_unit: Mb,
    soak_period_ms: 30_000,
    cool_period_ms: 60_000,
    default_alert_level: Warning,
    metric_buffer_capacity: 10_000,
    brain: None,
    metric_spillover_path: None,
    poll_jitter: None,
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

/// Set the hot-buffer capacity (number of recent metrics retained).
pub fn with_metric_buffer_capacity(config: Config, capacity: Int) -> Config {
  Config(..config, metric_buffer_capacity: capacity)
}

/// Stream buffered metrics to the registered process `name` on `node` (empty
/// `node` = local node). Enables the connection manager.
pub fn with_brain_stream(config: Config, node: String, name: String) -> Config {
  Config(..config, brain: Some(BrainStream(node: node, name: name)))
}

/// Enable the Dets cold-tier spillover at `path`. Metrics spill to this file
/// when the Brain connection is lost and the hot buffer fills, and are replayed
/// (disk first) on reconnect. Only effective alongside `with_brain_stream`.
pub fn with_metric_spillover(config: Config, path: String) -> Config {
  Config(..config, metric_spillover_path: Some(path))
}

/// Jitter polling by up to `ratio` of each statistic's interval (clamped to
/// `[0.0, 1.0]`). Each poller starts after a random delay in
/// `[0, interval * ratio)`, staggering traffic across a fleet so agents never
/// poll in lockstep.
pub fn with_poll_jitter(config: Config, ratio: Float) -> Config {
  Config(..config, poll_jitter: Some(float.clamp(ratio, 0.0, 1.0)))
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
    HostMemoryUsed(_) -> "host_memory_used"
    HostDiskUsed(_) -> "host_disk_used"
    HostCpuUtil(_) -> "host_cpu_util"
    SchedulerUtilization(_) -> "scheduler_utilization"
    // Distinct windows over the same inner threshold currently share an id.
    Windowed(over:, ..) -> "windowed_" <> threshold_id(over)
  }
}
