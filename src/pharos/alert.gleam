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
  )
}
