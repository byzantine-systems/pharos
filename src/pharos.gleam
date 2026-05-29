//// Public API.
////
//// `start_link/1` boots the entire pharos supervision tree from a
//// `Config`. The returned `Pharos` handle lets callers `subscribe` to
//// alert events, `unsubscribe` later, and `stop` everything when done.
////
//// ## Example
////
//// ```gleam
//// import pharos
//// import pharos/config
//// import pharos/statistic
////
//// let assert Ok(started) =
////   pharos.start_link(
////     config.new()
////     |> config.with_statistics([
////       statistic.poll(statistic.BeamMemory),
////       statistic.poll_every(statistic.BeamRunQueues, 500),
////     ])
////     |> config.with_thresholds([config.TotalMemory(above: 500.0)]),
////   )
////
//// let assert Ok(_handler) =
////   pharos.subscribe(started.data, fn(event) { handle(event) })
//// ```

import eparch/event_manager
import gleam/erlang/atom.{type Atom}
import gleam/erlang/process.{type Pid}
import gleam/list
import gleam/otp/actor
import pharos/alert.{type AlertEvent, AlertData}
import pharos/alert_manager
import pharos/config.{type Config}
import pharos/event_bus.{type EventBus, type HandlerId}
import pharos/internal/supervisor as pharos_supervisor
import pharos/internal/telemetry
import pharos/probe

/// Opaque handle returned by `start_link`. Pass it to `subscribe`,
/// `unsubscribe`, and `stop`.
pub opaque type Pharos {
  Pharos(supervisor_pid: Pid, bus: EventBus, handler_id: Atom)
}

/// Reasons `start_link/1` can fail.
pub type StartError {
  /// The OTP supervisor itself could not be started; carries the reason.
  SupervisorStartFailed(reason: String)
  /// The supervisor started but the telemetry handler could not be
  /// attached. The supervisor (and all its children) is shut down before
  /// returning this error so we don't leak processes.
  TelemetryAttachFailed(reason: String)
}

/// Boot pharos from `config`.
///
/// Generates stable registered names for the event bus and one per
/// threshold, builds the supervision tree, then attaches a single
/// telemetry handler that decodes events and routes
/// `breach`/`recover` casts to the matching alert managers.
pub fn start_link(config: Config) -> Result(actor.Started(Pharos), StartError) {
  let bus_name = process.new_name(prefix: "pharos_event_bus")

  // BEAM threshold lane: pair each threshold with its registered name.
  let beam_lane =
    list.map(config.thresholds, fn(threshold) {
      let id = config.threshold_id(threshold)
      #(threshold, process.new_name(prefix: "pharos_alert_" <> id))
    })
  // Custom probe threshold lane: same shape, distinct name prefix.
  let probe_lane =
    list.map(config.custom_thresholds, fn(threshold) {
      let id = probe.threshold_id(threshold)
      #(threshold, process.new_name(prefix: "pharos_probe_" <> id))
    })

  let manager_specs =
    list.append(
      list.map(beam_lane, fn(pair) {
        let #(threshold, name) = pair
        pharos_supervisor.AlertManagerSpec(
          data: AlertData(
            id: config.threshold_id(threshold),
            level: config.default_alert_level,
            soak_period_ms: config.soak_period_ms,
            cool_period_ms: config.cool_period_ms,
          ),
          name: name,
        )
      }),
      list.map(probe_lane, fn(pair) {
        let #(threshold, name) = pair
        pharos_supervisor.AlertManagerSpec(
          data: AlertData(
            id: probe.threshold_id(threshold),
            level: threshold.level,
            soak_period_ms: config.soak_period_ms,
            cool_period_ms: config.cool_period_ms,
          ),
          name: name,
        )
      }),
    )

  case pharos_supervisor.start_link(config, bus_name, manager_specs) {
    Error(error) -> Error(SupervisorStartFailed(format_actor_error(error)))
    Ok(started) -> {
      let bus = event_bus.from_name(bus_name)
      let handler_id = atom.create("pharos_handler")

      let manager_handles =
        list.map(beam_lane, fn(pair) {
          let #(threshold, name) = pair
          #(threshold, alert_manager.from_name(name))
        })
      let probe_handles =
        list.map(probe_lane, fn(pair) {
          let #(threshold, name) = pair
          #(threshold, alert_manager.from_name(name))
        })

      case
        telemetry.attach(
          config,
          manager_handles,
          probe_handles,
          config.custom_statistics,
          handler_id,
        )
      {
        Ok(Nil) ->
          Ok(actor.Started(
            pid: started.pid,
            data: Pharos(
              supervisor_pid: started.pid,
              bus: bus,
              handler_id: handler_id,
            ),
          ))
        Error(attach_error) -> {
          // Supervisor started but the handler couldn't attach: tear it
          // down so we don't leak processes.
          shutdown_supervisor(started.pid)
          Error(TelemetryAttachFailed(format_attach_error(attach_error)))
        }
      }
    }
  }
}

/// Subscribe `on_event` to the alert event bus. Returns an opaque handle
/// that can be passed to `unsubscribe`.
pub fn subscribe(
  pharos: Pharos,
  on_event: fn(AlertEvent) -> Nil,
) -> Result(HandlerId, event_manager.AddError(Nil, Nil)) {
  event_bus.add_handler(pharos.bus, on_event)
}

/// Unsubscribe a previously registered handler.
pub fn unsubscribe(
  pharos: Pharos,
  handler: HandlerId,
) -> Result(Nil, event_manager.RemoveError(Nil, Nil)) {
  event_bus.remove_handler(pharos.bus, handler)
}

/// Stop pharos: detach the telemetry handler, then shut down the
/// supervision tree. Safe to call multiple times - extra calls become
/// no-ops once the supervisor is already gone.
pub fn stop(pharos: Pharos) -> Nil {
  let _ = telemetry.detach(pharos.handler_id)
  shutdown_supervisor(pharos.supervisor_pid)
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

@external(erlang, "pharos_ffi", "shutdown_supervisor")
fn shutdown_supervisor(pid: Pid) -> Nil

fn format_actor_error(error: actor.StartError) -> String {
  case error {
    actor.InitTimeout -> "supervisor initialisation timed out"
    actor.InitFailed(reason) -> reason
    actor.InitExited(_) -> "supervisor exited during initialisation"
  }
}

fn format_attach_error(error: telemetry.AttachError) -> String {
  case error {
    telemetry.HandlerAlreadyAttached -> "telemetry handler already attached"
    telemetry.AttachFailed(reason: reason) -> reason
  }
}
