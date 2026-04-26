//// Telemetry handler attach / detach + routing.
////
//// `telemetry:attach_many/4` registers a global handler in an ETS table.
//// The handler is *not* a process; it has no place in the OTP supervision
//// tree on its own. We attach it once when `pharos.start_link` succeeds,
//// detach when `pharos.stop` is called, and rely on registered names to
//// keep the routing callback's references valid across alert manager
//// restarts.

import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom.{type Atom}
import gleam/list
import pharos/alert_manager.{type AlertManager}
import pharos/config.{type Config, type Threshold}
import pharos/measurement
import pharos/statistic
import pharos/threshold_eval

// ---------------------------------------------------------------------------
// Typed errors
// ---------------------------------------------------------------------------

pub type AttachError {
  HandlerAlreadyAttached
  AttachFailed(reason: String)
}

pub type DetachError {
  HandlerNotFound
  DetachFailed(reason: String)
}

// ---------------------------------------------------------------------------
// Telemetry event (input shape produced by pharos_ffi:attach_many/4)
// ---------------------------------------------------------------------------

pub type TelemetryEvent {
  TelemetryEvent(
    name: List(Atom),
    measurements: Dict(Atom, Dynamic),
    metadata: Dict(Atom, Dynamic),
  )
}

// ---------------------------------------------------------------------------
// Public: attach / detach
// ---------------------------------------------------------------------------

/// Attach a single telemetry handler that decodes events and routes
/// breach/recover signals to the matching `AlertManager`.
///
/// The handler subscribes to the union of every event name implied by
/// `config.statistics`, so no event-name configuration leaks to the
/// caller.
pub fn attach(
  config: Config,
  managers: List(#(Threshold, AlertManager)),
  handler_id: Atom,
) -> Result(Nil, AttachError) {
  let event_names = derive_event_names(config)
  let memory_unit = config.memory_unit
  let thresholds = managers

  let on_event = fn(event: TelemetryEvent, _config: Nil) -> Nil {
    let decoded =
      measurement.decode_with_unit(
        measurement.TelemetryEvent(
          name: event.name,
          measurements: event.measurements,
          metadata: event.metadata,
        ),
        memory_unit,
      )
    case decoded {
      Ok(decoded_measurement) -> route(decoded_measurement, thresholds)
      Error(_) -> Nil
    }
  }

  attach_many(handler_id, event_names, on_event, Nil)
}

/// Detach a previously attached handler.
pub fn detach(handler_id: Atom) -> Result(Nil, DetachError) {
  detach_handler(handler_id)
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

fn route(
  decoded_measurement: measurement.Measurement,
  managers: List(#(Threshold, AlertManager)),
) -> Nil {
  list.each(managers, fn(pair) {
    let #(threshold, manager) = pair
    case threshold_eval.evaluate(decoded_measurement, threshold) {
      threshold_eval.NotApplicable -> Nil
      threshold_eval.Breached -> alert_manager.breach(manager)
      threshold_eval.Healthy -> alert_manager.recover(manager)
    }
  })
}

// ---------------------------------------------------------------------------
// Event-name derivation
//
// Each StatisticKind implies one telemetry event name. We collect the union
// across the configured statistics so the handler subscribes to exactly
// what's being polled.
// ---------------------------------------------------------------------------

fn derive_event_names(config: Config) -> List(List(Atom)) {
  config.statistics
  |> list.map(fn(stat) { event_name_for(stat.kind) })
  |> dedupe
}

fn event_name_for(kind: statistic.StatisticKind) -> List(Atom) {
  case kind {
    statistic.BeamMemory -> [atom.create("vm"), atom.create("memory")]
    statistic.BeamRunQueues -> [
      atom.create("vm"),
      atom.create("total_run_queue_lengths"),
    ]
    statistic.BeamSystemCounts -> [
      atom.create("vm"),
      atom.create("system_counts"),
    ]
    statistic.BeamPersistentTerm -> [
      atom.create("vm"),
      atom.create("persistent_term"),
    ]
    statistic.ProcessInfo(_, event, _) -> event
    statistic.ClusterNodes -> [
      atom.create("pharos"),
      atom.create("cluster"),
      atom.create("nodes"),
    ]
    statistic.HostMemory -> [
      atom.create("pharos"),
      atom.create("host"),
      atom.create("memory"),
    ]
    statistic.HostDisk -> [
      atom.create("pharos"),
      atom.create("host"),
      atom.create("disk"),
    ]
  }
}

fn dedupe(items: List(List(Atom))) -> List(List(Atom)) {
  list.fold(items, [], fn(acc, item) {
    case list.contains(acc, item) {
      True -> acc
      False -> [item, ..acc]
    }
  })
}

// ---------------------------------------------------------------------------
// FFI
// ---------------------------------------------------------------------------

@external(erlang, "pharos_ffi", "attach_many")
fn attach_many(
  id: Atom,
  events: List(List(Atom)),
  handler: fn(TelemetryEvent, config) -> Nil,
  config: config,
) -> Result(Nil, AttachError)

@external(erlang, "pharos_ffi", "detach")
fn detach_handler(id: Atom) -> Result(Nil, DetachError)
