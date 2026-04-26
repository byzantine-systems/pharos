//// The pharos supervision tree.
////
//// ```
//// pharos_sup (rest_for_one)
//// ├── event_bus       (worker, eparch event_manager registered as `bus_name`)
//// ├── hot_buffer      (worker, owns the ETS metric ring)
//// ├── spillover       (worker, optional, owns the Dets cold-tier file)
//// ├── alert_supervisor (one_for_one supervisor)
//// │     └── alert_manager x N (each registered under its own name)
//// ├── poller_supervisor (one_for_one supervisor)
//// │     └── telemetry_poller x M (one per distinct interval bucket)
//// └── connection      (worker, optional Brain metric-stream gen_statem)
//// ```
////
//// `rest_for_one`: if the bus dies, everything downstream is restarted along
//// with it (their callbacks reference the bus by name, which is unregistered
//// while the bus is dead). The hot buffer and the optional spillover store sit
//// before the pollers and the connection so their ETS table / Dets file exist
//// before the telemetry handler pushes and the connection drains. The
//// connection manager is last: it drains the buffers, so its own crashes never
//// disturb collection, while a buffer crash restarts it (and re-derives its
//// handle).

import eparch/event_manager
import eparch/state_machine
import gleam/erlang/process.{type Name}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/static_supervisor as supervisor
import gleam/otp/supervision.{type ChildSpecification}
import pharos/alert.{type AlertData, type AlertEvent}
import pharos/alert_manager.{type AlertManager, type Message}
import pharos/config.{type Config}
import pharos/event_bus.{type EventBus}
import pharos/internal/connection.{type Connection}
import pharos/internal/hot_buffer.{type HotBuffer}
import pharos/internal/poller
import pharos/internal/spillover.{type Spillover}

/// One row in the alert-manager spec table. Carries the fully-resolved
/// `AlertData` (built by the caller from either a BEAM `Threshold` or a
/// `ProbeThreshold`) plus the `name` to register the state machine
/// deterministically. The implicit ordering of the list fixes the
/// static_supervisor child id, so both lanes share one uniform table.
pub type AlertManagerSpec {
  AlertManagerSpec(data: AlertData, name: Name(Message))
}

// ---------------------------------------------------------------------------
// Public: build and start the supervisor
// ---------------------------------------------------------------------------

pub fn start_link(
  config: Config,
  bus_name: Name(AlertEvent),
  manager_specs: List(AlertManagerSpec),
  buffer_child: ChildSpecification(HotBuffer),
  spillover_child: Option(ChildSpecification(Spillover)),
  connection_child: Option(ChildSpecification(Connection)),
) -> Result(actor.Started(supervisor.Supervisor), actor.StartError) {
  let with_spillover = case spillover_child {
    None ->
      supervisor.new(supervisor.RestForOne)
      |> supervisor.add(event_bus_child(bus_name))
      |> supervisor.add(buffer_child)
    Some(child) ->
      supervisor.new(supervisor.RestForOne)
      |> supervisor.add(event_bus_child(bus_name))
      |> supervisor.add(buffer_child)
      |> supervisor.add(child)
  }

  let base =
    with_spillover
    |> supervisor.add(alert_supervisor_child(bus_name, manager_specs))
    |> supervisor.add(poller_supervisor_child(config))

  case connection_child {
    None -> base
    Some(child) -> supervisor.add(base, child)
  }
  |> supervisor.start
}

// ---------------------------------------------------------------------------
// Children
// ---------------------------------------------------------------------------

fn event_bus_child(bus_name: Name(AlertEvent)) -> ChildSpecification(EventBus) {
  supervision.worker(fn() {
    case event_bus.start_link(bus_name) {
      Ok(#(pid, bus)) -> Ok(actor.Started(pid: pid, data: bus))
      Error(error) ->
        Error(actor.InitFailed(event_manager_error_to_string(error)))
    }
  })
}

fn alert_supervisor_child(
  bus_name: Name(AlertEvent),
  manager_specs: List(AlertManagerSpec),
) -> ChildSpecification(supervisor.Supervisor) {
  let bus = event_bus.from_name(bus_name)
  let inner =
    list.fold(
      manager_specs,
      supervisor.new(supervisor.OneForOne),
      fn(builder, spec) {
        let AlertManagerSpec(data:, name:) = spec
        supervisor.add(builder, alert_manager_child(data, bus, name))
      },
    )
  supervisor.supervised(inner)
}

fn alert_manager_child(
  data: AlertData,
  bus: EventBus,
  name: Name(Message),
) -> ChildSpecification(AlertManager) {
  supervision.worker(fn() {
    case alert_manager.start_link(data, bus, name) {
      Ok(handle) ->
        case process.named(name) {
          Ok(pid) -> Ok(actor.Started(pid: pid, data: handle))
          Error(_) ->
            Error(actor.InitFailed(
              "alert manager registered name not found after start",
            ))
        }
      Error(error) ->
        Error(actor.InitFailed(state_machine_error_to_string(error)))
    }
  })
}

fn poller_supervisor_child(
  config: Config,
) -> ChildSpecification(supervisor.Supervisor) {
  let specs =
    list.append(
      poller.child_specs(config.statistics, config.poll_jitter),
      poller.probe_child_specs(config.custom_statistics, config.poll_jitter),
    )
  let inner =
    list.fold(specs, supervisor.new(supervisor.OneForOne), fn(builder, spec) {
      supervisor.add(builder, spec)
    })
  supervisor.supervised(inner)
}

// ---------------------------------------------------------------------------
// Cross-package error -> string
// ---------------------------------------------------------------------------

fn event_manager_error_to_string(error: event_manager.StartError) -> String {
  case error {
    event_manager.AlreadyStarted(_) -> "event bus already started"
    event_manager.StartFailed(reason: reason) -> reason
  }
}

fn state_machine_error_to_string(error: state_machine.StartError) -> String {
  case error {
    state_machine.InitTimeout -> "alert manager init timeout"
    state_machine.InitFailed(reason) -> reason
    state_machine.InitExited(_) -> "alert manager init exited"
    state_machine.AlreadyStarted(_) -> "alert manager name already registered"
  }
}
