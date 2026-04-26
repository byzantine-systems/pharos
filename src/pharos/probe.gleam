//// Custom monitoring probes - the pluggable lane of pharos.
////
//// The built-in `Statistic` / `Measurement` / `Threshold` enums cover BEAM
//// and host signals. A `Probe` opens the same 
////    `poll` -> `decode` -> `threshold` -> `alert` 
//// pipeline to *any* signal a caller can express, without modifying
//// pharos: it bundles how to poll (a `Source`), the telemetry event it
//// emits, and how to decode that event into named numeric readings
//// (`MetricSample`). `ProbeThreshold`s compare a single sample field against
//// a limit and feed the same soak/cool alert machinery the BEAM lane uses.
////
//// This is what lets a consumer such as `logothetes` monitor MicroVMs:
//// register a probe whose `Source` points at an emitter that reads
//// `/proc/<pid>` and `telemetry:execute`s the readings, plus thresholds over
//// the decoded fields.

import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom.{type Atom}
import pharos/alert.{type AlertLevel}
import pharos/measurement.{type TelemetryEvent}

/// A single probe reading reduced to named numeric scalars (`field -> value`).
/// Thresholds select a field by name and compare it against a limit.
pub type MetricSample =
  Dict(String, Float)

/// How `telemetry_poller` drives a probe.
///
/// `CustomMfa` lowers to the poller's `{module, function, args}` measurement:
/// the function is invoked every polling cycle and is expected to call
/// `telemetry:execute/3` itself with the probe's event name.
pub type Source {
  CustomMfa(module: Atom, function: Atom, args: List(Dynamic))
}

/// A pluggable probe. Construct with `new/5`.
pub opaque type Probe {
  Probe(
    id: String,
    event_name: List(Atom),
    interval_ms: Int,
    source: Source,
    decode: fn(TelemetryEvent) -> Result(MetricSample, String),
  )
}

/// Define a probe.
///
/// - `id` - stable identifier, referenced by `ProbeThreshold.probe_id`.
/// - `event_name` - the telemetry event the `source` emits (e.g.
///   `[atom.create("logothetes"), atom.create("vm"), atom.create("metrics")]`).
/// - `interval_ms` - how often to poll.
/// - `source` - how the reading is produced.
/// - `decode` - lift the raw telemetry event into a `MetricSample`.
pub fn new(
  id id: String,
  event_name event_name: List(Atom),
  interval_ms interval_ms: Int,
  source source: Source,
  decode decode: fn(TelemetryEvent) -> Result(MetricSample, String),
) -> Probe {
  Probe(
    id: id,
    event_name: event_name,
    interval_ms: interval_ms,
    source: source,
    decode: decode,
  )
}

/// The probe's stable identifier.
pub fn id(probe: Probe) -> String {
  probe.id
}

/// The telemetry event name the probe emits.
pub fn event_name(probe: Probe) -> List(Atom) {
  probe.event_name
}

/// The probe's polling interval in milliseconds.
pub fn interval_ms(probe: Probe) -> Int {
  probe.interval_ms
}

/// How the probe is polled.
pub fn source(probe: Probe) -> Source {
  probe.source
}

/// Decode a raw telemetry event into a `MetricSample` using the probe's
/// decoder.
pub fn decode(
  probe: Probe,
  event: TelemetryEvent,
) -> Result(MetricSample, String) {
  probe.decode(event)
}

/// Comparison applied by a `ProbeThreshold` to a sample field.
pub type Comparison {
  Above(limit: Float)
  Below(limit: Float)
}

/// A threshold over one field of a probe's `MetricSample`.
pub type ProbeThreshold {
  ProbeThreshold(
    /// The `Probe.id` whose sample this threshold reads.
    probe_id: String,
    /// Which `MetricSample` key to compare.
    field: String,
    /// How to compare the field value against the limit.
    comparison: Comparison,
    /// Severity carried in `AlertFiring` events for this threshold.
    level: AlertLevel,
  )
}

/// Stable string id derived from a probe threshold. Used to register the
/// per-threshold alert manager under a deterministic name (mirrors
/// `config.threshold_id` for the BEAM lane).
pub fn threshold_id(threshold: ProbeThreshold) -> String {
  let comparison = case threshold.comparison {
    Above(_) -> "above"
    Below(_) -> "below"
  }
  threshold.probe_id <> "_" <> threshold.field <> "_" <> comparison
}
