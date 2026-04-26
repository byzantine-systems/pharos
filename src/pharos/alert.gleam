import gleam/int
import gleam/list
import gleam/option.{type Option}

/// Alert severity level attached to a firing alert.
pub type AlertLevel {
  Warning
  Critical
}

/// Events broadcast over the gen_event alert dispatcher bus.
pub type AlertEvent {
  /// Threshold has been continuously breached for longer than `soak_period_ms`.
  /// `diagnostic` is a snapshot of VM state captured at the moment the soak
  /// timer fires.
  AlertFiring(id: String, level: AlertLevel, diagnostic: String)
  /// System recovered and stayed healthy for at least `cool_period_ms`.
  AlertResolved(id: String)
}

/// The four states of the alert lifecycle state machine.
pub type AlertState {
  /// System is healthy; no timer running.
  Clear
  /// Threshold just breached; soak timer is counting down.
  Pending
  /// Alert is active - threshold was breached for the full soak period.
  Firing
  /// System recovered; cool-down timer is counting down.
  Cooling
}

/// Configuration for a single alert rule gen_statem instance.
/// Pass to `alert_manager.start_link/2`.
pub type AlertData {
  AlertData(
    /// Unique string ID for this alert rule (e.g. `"total_memory"`).
    id: String,
    /// Severity level carried in `AlertFiring` events.
    level: AlertLevel,
    /// Milliseconds the threshold must stay breached before the alert fires.
    /// Set to `0` to skip the `Pending` state and fire immediately.
    soak_period_ms: Int,
    /// Milliseconds recovery must hold before the alert resolves.
    cool_period_ms: Int,
    /// When `Some`, the manager evaluates a sliding window of samples instead
    /// of a per-tick breach signal. Windowed rules use `soak_period_ms: 0`
    /// (the window is the debounce); `cool_period_ms` still applies.
    window: Option(WindowSpec),
  )
}

// ---------------------------------------------------------------------------
// Sliding Window
// ---------------------------------------------------------------------------

/// How a sliding window aggregates its samples into a breach decision.
pub type WindowMode {
  /// Breached when the mean of the window's values exceeds `limit`.
  Average
  /// Breached when the fraction of samples exceeding `limit` is at least
  /// `at_least` (a ratio in `[0.0, 1.0]`).
  Percentage(at_least: Float)
}

/// Runtime parameters for a windowed alert rule. `limit` is the inner
/// threshold's comparison limit (widened to a float); `window_ms` is the
/// window duration; `mode` selects average vs percentage aggregation.
pub type WindowSpec {
  WindowSpec(window_ms: Int, limit: Float, mode: WindowMode)
}

/// Drop samples (a `#(timestamp_ms, value)` list) older than `now - window_ms`.
pub fn prune_samples(
  samples: List(#(Int, Float)),
  now: Int,
  window_ms: Int,
) -> List(#(Int, Float)) {
  let cutoff = now - window_ms
  list.filter(samples, fn(sample) { sample.0 >= cutoff })
}

/// Whether the window currently breaches per `spec`, considering only samples
/// within `[now - window_ms, now]`. An empty window is never breaching.
pub fn window_breached(
  samples: List(#(Int, Float)),
  now: Int,
  spec: WindowSpec,
) -> Bool {
  let values =
    prune_samples(samples, now, spec.window_ms)
    |> list.map(fn(sample) { sample.1 })
  case values {
    [] -> False
    _ ->
      case spec.mode {
        Average -> mean(values) >. spec.limit
        Percentage(at_least: ratio) -> {
          let breaching = list.count(values, fn(value) { value >. spec.limit })
          int.to_float(breaching) /. int.to_float(list.length(values)) >=. ratio
        }
      }
  }
}

fn mean(values: List(Float)) -> Float {
  let sum = list.fold(values, 0.0, fn(acc, value) { acc +. value })
  sum /. int.to_float(list.length(values))
}
