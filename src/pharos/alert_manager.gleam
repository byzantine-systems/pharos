//// Per-threshold alert state machine.
////
//// Wraps an `eparch/state_machine` running the four-state lifecycle
//// `Clear → Pending → Firing → Cooling → Clear`. The state machine is
//// registered under a deterministic `Name` so that supervisor restarts
//// keep `AlertManager` handles valid: callers send `Breach` / `Recover`
//// casts to the registered name, which the OTP runtime resolves to the
//// current pid on each call.
////
//// `breach/1` and `recover/1` map physical signals (threshold crossed
//// up / down) to state transitions. The dispatcher is notified with
//// `AlertFiring` and `AlertResolved` events on the relevant transitions.

import eparch/state_machine.{type StartError}
import gleam/erlang/process.{type Name}
import pharos/alert.{
  type AlertData, type AlertLevel, type AlertState, AlertData, AlertFiring,
  AlertResolved, Clear, Cooling, Firing, Pending,
}
import pharos/event_bus.{type EventBus}

/// Restart-resilient handle to a per-threshold alert state machine.
pub opaque type AlertManager {
  AlertManager(name: Name(Message))
}

/// Internal command messages.
pub type Message {
  Breach
  Recover
}

/// Internal data carried across state transitions.
type Data {
  Data(
    id: String,
    level: AlertLevel,
    soak_milliseconds: Int,
    cool_milliseconds: Int,
    bus: EventBus,
  )
}

/// Wrap a registered name as an `AlertManager`. The supervisor uses this
/// to build handles before the underlying state machine has been started,
/// so that the handler-attacher can route breach/recover signals to a
/// stable address.
pub fn from_name(name: Name(Message)) -> AlertManager {
  AlertManager(name: name)
}

/// Start a state machine for `data`, registering it under `name`. Notifies
/// `bus` with `AlertFiring` / `AlertResolved` events on the relevant
/// transitions.
pub fn start_link(
  data: AlertData,
  bus: EventBus,
  name: Name(Message),
) -> Result(AlertManager, StartError) {
  let AlertData(id:, level:, soak_period_ms:, cool_period_ms:) = data
  let initial =
    Data(
      id: id,
      level: level,
      soak_milliseconds: soak_period_ms,
      cool_milliseconds: cool_period_ms,
      bus: bus,
    )

  let result =
    state_machine.new(initial_state: Clear, initial_data: initial)
    |> state_machine.named(name)
    |> state_machine.on_event(handle_event)
    |> state_machine.start

  case result {
    Ok(_started) -> Ok(AlertManager(name: name))
    Error(error) -> Error(error)
  }
}

/// Signal that the threshold has been breached.
pub fn breach(manager: AlertManager) -> Nil {
  state_machine.cast(process.named_subject(manager.name), Breach)
}

/// Signal that the threshold is no longer breached.
pub fn recover(manager: AlertManager) -> Nil {
  state_machine.cast(process.named_subject(manager.name), Recover)
}

fn handle_event(
  event: state_machine.Event(AlertState, Message, reply),
  state: AlertState,
  data: Data,
) -> state_machine.Step(AlertState, Data, Message, reply) {
  case event {
    state_machine.Cast(Breach) -> handle_breach(state, data)
    state_machine.Cast(Recover) -> handle_recover(state, data)
    state_machine.Timeout(state_machine.StateTimeoutType) ->
      handle_state_timeout(state, data)
    state_machine.Timeout(state_machine.GenericTimeoutType(_)) ->
      state_machine.keep_state(data, [])
    state_machine.Info(_) -> state_machine.keep_state(data, [])
    state_machine.Call(_, _) -> state_machine.keep_state(data, [])
    state_machine.Enter(_) -> state_machine.keep_state(data, [])
  }
}

fn handle_breach(
  state: AlertState,
  data: Data,
) -> state_machine.Step(AlertState, Data, Message, reply) {
  case state {
    Clear ->
      case data.soak_milliseconds {
        0 -> {
          fire(data)
          state_machine.next_state(Firing, data, [])
        }
        milliseconds ->
          state_machine.next_state(Pending, data, [
            state_machine.StateTimeout(milliseconds),
          ])
      }
    Pending -> state_machine.keep_state(data, [])
    Firing -> state_machine.keep_state(data, [])
    Cooling -> {
      fire(data)
      state_machine.next_state(Firing, data, [])
    }
  }
}

fn handle_recover(
  state: AlertState,
  data: Data,
) -> state_machine.Step(AlertState, Data, Message, reply) {
  case state {
    Clear -> state_machine.keep_state(data, [])
    Pending -> state_machine.next_state(Clear, data, [])
    Firing ->
      state_machine.next_state(Cooling, data, [
        state_machine.StateTimeout(data.cool_milliseconds),
      ])
    Cooling -> state_machine.keep_state(data, [])
  }
}

fn handle_state_timeout(
  state: AlertState,
  data: Data,
) -> state_machine.Step(AlertState, Data, Message, reply) {
  case state {
    Clear -> state_machine.keep_state(data, [])
    Pending -> {
      fire(data)
      state_machine.next_state(Firing, data, [])
    }
    Firing -> state_machine.keep_state(data, [])
    Cooling -> {
      event_bus.notify(data.bus, AlertResolved(id: data.id))
      state_machine.next_state(Clear, data, [])
    }
  }
}

fn fire(data: Data) -> Nil {
  event_bus.notify(
    data.bus,
    AlertFiring(
      id: data.id,
      level: data.level,
      diagnostic: collect_diagnostic(),
    ),
  )
}

@external(erlang, "pharos_ffi", "collect_diagnostic")
fn collect_diagnostic() -> String
