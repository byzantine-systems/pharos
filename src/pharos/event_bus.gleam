//// The alert event bus.
////
//// A thin, restart-resilient wrapper around an `eparch/event_manager`
//// configured with a registered name. The wrapper holds the `Name`, not
//// a `Pid` or `Manager` value, so handles stay valid across supervisor
//// restarts: when the underlying gen_event process is restarted under
//// the same registered name, every previously-issued `EventBus` handle
//// keeps working transparently.
////
//// Most callers should not interact with `EventBus` directly: it is
//// constructed by the pharos supervisor and exposed indirectly via
//// `pharos.subscribe` / `pharos.unsubscribe`.

import eparch/event_manager.{
  type AddError, type HandlerRef, type Manager, type RemoveError,
  type StartError,
}
import gleam/erlang/process.{type Name, type Pid}
import pharos/alert.{type AlertEvent}

/// Restart-resilient handle to a running alert event bus.
pub opaque type EventBus {
  EventBus(name: Name(AlertEvent))
}

/// Opaque reference to a registered handler. Returned by `add_handler/2`;
/// pass to `remove_handler/2` to unsubscribe.
pub opaque type HandlerId {
  HandlerId(ref: HandlerRef)
}

/// Wrap a registered name as an `EventBus`. The name must already (or
/// eventually) be registered to a running `gen_event` manager process;
/// the wrapper resolves it lazily on every API call.
pub fn from_name(name: Name(AlertEvent)) -> EventBus {
  EventBus(name: name)
}

/// Start a fresh event bus registered under `name`.
///
/// Used by the supervisor's bus child. The returned `Pid` is the bus's
/// process id (suitable for `actor.Started`); the bus is also registered
/// under `name` so other processes can reach it after a restart.
pub fn start_link(
  name: Name(AlertEvent),
) -> Result(#(Pid, EventBus), StartError) {
  let options =
    event_manager.new_start_options()
    |> event_manager.with_name(name)
  case event_manager.start_monitor(options) {
    Ok(monitored) -> {
      let pid = event_manager.manager_pid(monitored.manager)
      Ok(#(pid, EventBus(name: name)))
    }
    Error(error) -> Error(error)
  }
}

/// Broadcast `event` to every registered handler. Non-blocking.
pub fn notify(bus: EventBus, event: AlertEvent) -> Nil {
  event_manager.notify(manager_for(bus), event)
}

/// Register a Gleam function as an event handler on the bus.
pub fn add_handler(
  bus: EventBus,
  handler: fn(AlertEvent) -> Nil,
) -> Result(HandlerId, AddError) {
  let handler_spec =
    event_manager.new_handler(initial_state: Nil, on_event: fn(event, _state) {
      handler(event)
      event_manager.Continue(Nil)
    })
  case event_manager.add_handler(manager_for(bus), handler_spec) {
    Ok(ref) -> Ok(HandlerId(ref))
    Error(error) -> Error(error)
  }
}

/// Unregister a previously added handler.
pub fn remove_handler(bus: EventBus, id: HandlerId) -> Result(Nil, RemoveError) {
  event_manager.remove_handler(manager_for(bus), id.ref)
}

/// Resolve the bus's `Name(AlertEvent)` to an eparch `Manager(AlertEvent)`.
/// At the Erlang level both are interchangeable references that gen_event
/// resolves at call time, so the FFI shim is an identity function on the
/// underlying atom.
fn manager_for(bus: EventBus) -> Manager(AlertEvent) {
  manager_from_name(bus.name)
}

@external(erlang, "pharos_ffi", "manager_from_name")
fn manager_from_name(name: Name(AlertEvent)) -> Manager(AlertEvent)
