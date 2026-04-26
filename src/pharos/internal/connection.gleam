//// The "Brain" connection manager
////
//// An `eparch/state_machine` that owns delivery of metrics/alerts to the
//// central "Brain" (or any remote sink) and degrades gracefully under network
//// partitions:
////
//// ```
//// Disconnected ──tick──▶ Connecting ──ok──▶ Streaming
////      ▲                     │                  │
////      └──────fail───────────┘                  │ over capacity
////      ▲                                         ▼
////      └──────────conn lost──────────────────Throttled
//// ```
////
//// - **Disconnected** buffers everything in the hot buffer and arms a retry
////   timer using exponential backoff **with jitter** (so a fleet doesn't
////   reconnect in lockstep).
//// - **Connecting** performs one connection attempt; success flushes the
////   buffer and moves to Streaming, failure bumps the backoff and returns to
////   Disconnected.
//// - **Streaming** delivers each metric as it arrives, flushing the buffer
////   first. A lost connection drops back to Disconnected (re-buffering).
//// - **Throttled** is entered when the buffer outruns `max_pending`; it paces
////   draining and returns to Streaming once it has caught up.
////
//// The transport is injected (`Transport`) so the manager is testable without
//// a real Brain and so alternative transports (e.g. OTLP) can reuse the FSM.

import eparch/state_machine.{
  type StartError, After, StateTimeout, StateTimeoutType,
}
import gleam/erlang/process.{type Name, type Pid}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision.{type ChildSpecification}
import pharos/internal/hot_buffer.{type HotBuffer}
import pharos/internal/spillover.{type Spillover}
import pharos/metric.{type Metric}

/// The four lifecycle states (also the FSM's `state` value).
pub type ConnectionState {
  Disconnected
  Connecting
  Streaming
  Throttled
}

/// Tuning knobs for reconnection backoff and backpressure.
pub type ConnectionConfig {
  ConnectionConfig(
    /// Base backoff delay in milliseconds (first retry, pre-jitter).
    base_ms: Int,
    /// Maximum backoff delay in milliseconds (cap on exponential growth).
    max_ms: Int,
    /// Buffer depth above which the manager enters `Throttled`.
    max_pending: Int,
    /// How often (ms) to drain the buffer while `Streaming`, so metrics pushed
    /// straight to the buffer (e.g. by the telemetry handler) are shipped
    /// without an explicit `deliver` cast.
    flush_interval_ms: Int,
  )
}

/// Pluggable delivery transport. `connect` establishes/verifies reachability;
/// `deliver` ships one metric. Both return `Error(reason)` on failure.
pub type Transport {
  Transport(
    connect: fn() -> Result(Nil, String),
    deliver: fn(Metric) -> Result(Nil, String),
  )
}

/// Restart-resilient handle to the connection manager.
pub opaque type Connection {
  Connection(name: Name(Message))
}

/// FSM messages. `Deliver` is the only external command; the rest are
/// self-armed timer payloads.
pub type Message {
  /// Enqueue a metric for delivery to the Brain.
  Deliver(Metric)
  /// Disconnected retry timer fired: time to attempt a connection.
  Tick
  /// Connecting entry timer fired: perform the actual connection attempt.
  Attempt
  /// Throttled pacing timer fired: try to drain the backlog.
  DrainTick
  /// Streaming periodic timer fired: drain whatever the buffer has accrued.
  FlushTick
}

type Data {
  Data(
    transport: Transport,
    buffer: HotBuffer,
    /// Disk-backed cold tier. `None` keeps the manager RAM-only; `Some` spills
    /// the hot buffer to disk during partitions and replays it first on
    /// reconnect.
    spillover: Option(Spillover),
    config: ConnectionConfig,
    /// Consecutive failed connection attempts; drives the backoff curve.
    attempt: Int,
  )
}

/// Wrap a registered name as a handle (mirrors the alert_manager pattern), so
/// the handle stays valid across supervisor restarts.
pub fn from_name(name: Name(Message)) -> Connection {
  Connection(name: name)
}

/// Start the connection manager registered under `name`, delivering through
/// `transport` and buffering in `buffer`. `spillover` is the optional disk
/// cold tier (`None` = RAM-only).
pub fn start_link(
  name: Name(Message),
  transport: Transport,
  buffer: HotBuffer,
  spillover: Option(Spillover),
  config: ConnectionConfig,
) -> Result(Connection, StartError) {
  case do_start(name, transport, buffer, spillover, config) {
    Ok(#(_pid, connection)) -> Ok(connection)
    Error(error) -> Error(error)
  }
}

/// A supervised worker running the connection manager.
pub fn child_spec(
  name: Name(Message),
  transport: Transport,
  buffer: HotBuffer,
  spillover: Option(Spillover),
  config: ConnectionConfig,
) -> ChildSpecification(Connection) {
  supervision.worker(fn() {
    case do_start(name, transport, buffer, spillover, config) {
      Ok(#(pid, connection)) -> Ok(actor.Started(pid: pid, data: connection))
      Error(error) -> Error(actor.InitFailed(start_error_to_string(error)))
    }
  })
}

fn do_start(
  name: Name(Message),
  transport: Transport,
  buffer: HotBuffer,
  spillover: Option(Spillover),
  config: ConnectionConfig,
) -> Result(#(Pid, Connection), StartError) {
  let initial = Data(transport:, buffer:, spillover:, config:, attempt: 0)
  let result =
    state_machine.new(initial_state: Disconnected, initial_data: initial)
    |> state_machine.named(state_machine.Local(name))
    |> state_machine.on_event(handle_event)
    |> state_machine.start_link

  case result {
    // Kick the lifecycle: arm the first (immediate) connection attempt.
    Ok(started) -> {
      let connection = Connection(name: name)
      schedule_first_tick(connection)
      Ok(#(started.pid, connection))
    }
    Error(error) -> Error(error)
  }
}

fn start_error_to_string(error: StartError) -> String {
  case error {
    state_machine.InitTimeout -> "connection manager init timeout"
    state_machine.InitFailed(reason) -> reason
    state_machine.InitExited(_) -> "connection manager init exited"
    state_machine.AlreadyStarted(_) ->
      "connection manager name already registered"
  }
}

/// Enqueue `metric` for delivery. Non-blocking.
pub fn deliver(connection: Connection, metric: Metric) -> Nil {
  state_machine.cast(server_ref(connection), Deliver(metric))
}

fn schedule_first_tick(connection: Connection) -> Nil {
  state_machine.cast(server_ref(connection), Tick)
}

fn server_ref(connection: Connection) -> state_machine.ServerRef(Message) {
  state_machine.ref_from_subject(process.named_subject(connection.name))
}

// ---------------------------------------------------------------------------
// Event handling
// ---------------------------------------------------------------------------

fn handle_event(
  event: state_machine.Event(ConnectionState, Message, reply),
  state: ConnectionState,
  data: Data,
) -> state_machine.Step(ConnectionState, Data, Message, reply) {
  case event {
    state_machine.Cast(Deliver(metric)) -> on_deliver(state, data, metric)
    state_machine.Cast(Tick) -> on_tick(state, data)
    state_machine.Cast(Attempt) -> on_attempt(state, data)
    state_machine.Cast(DrainTick) -> on_drain_tick(state, data)
    state_machine.Cast(FlushTick) -> on_flush_tick(state, data)
    state_machine.Timeout(StateTimeoutType, Tick) -> on_tick(state, data)
    state_machine.Timeout(StateTimeoutType, Attempt) -> on_attempt(state, data)
    state_machine.Timeout(StateTimeoutType, DrainTick) ->
      on_drain_tick(state, data)
    state_machine.Timeout(StateTimeoutType, FlushTick) ->
      on_flush_tick(state, data)
    // Inert: we only ever arm state timeouts with the payloads above.
    state_machine.Timeout(_, _) -> state_machine.keep_state(data, [])
    state_machine.Info(_) -> state_machine.keep_state(data, [])
    state_machine.Call(_, _) -> state_machine.keep_state(data, [])
    state_machine.Enter(_) -> state_machine.keep_state(data, [])
  }
}

fn on_deliver(
  state: ConnectionState,
  data: Data,
  metric: Metric,
) -> state_machine.Step(ConnectionState, Data, Message, reply) {
  case state {
    // Buffering states: spill the ring to disk before it can evict, then keep
    // the new metric in RAM. The connect path will replay disk-then-RAM.
    Disconnected | Connecting | Throttled -> {
      buffer_with_spill(data, metric)
      state_machine.keep_state(data, [])
    }
    Streaming -> {
      let _overflow = hot_buffer.push(data.buffer, metric)
      case flush(data) {
        Ok(Nil) -> maybe_throttle(data)
        Error(_) -> go_disconnected(data)
      }
    }
  }
}

/// Push `metric` into the hot buffer while buffering. With a cold tier enabled,
/// once the ring reaches the `max_pending` high-watermark its whole contents
/// are drained to disk *before* the next push could evict the oldest entry, so
/// a long partition never loses data to the bounded ring.
fn buffer_with_spill(data: Data, metric: Metric) -> Nil {
  case data.spillover {
    Some(sp) ->
      case hot_buffer.size(data.buffer) >= data.config.max_pending {
        True -> spillover.push_all(sp, hot_buffer.drain(data.buffer))
        False -> Nil
      }
    None -> Nil
  }
  let _overflow = hot_buffer.push(data.buffer, metric)
  Nil
}

fn on_tick(
  state: ConnectionState,
  data: Data,
) -> state_machine.Step(ConnectionState, Data, Message, reply) {
  case state {
    // Move into Connecting and fire the attempt immediately.
    Disconnected ->
      state_machine.next_state(Connecting, data, [
        StateTimeout(After(0), Attempt),
      ])
    // A retry tick that arrives in any other state is stale; ignore it.
    Connecting | Streaming | Throttled -> state_machine.keep_state(data, [])
  }
}

fn on_attempt(
  state: ConnectionState,
  data: Data,
) -> state_machine.Step(ConnectionState, Data, Message, reply) {
  case state {
    Connecting ->
      case data.transport.connect() {
        Error(_) -> go_disconnected(data)
        Ok(Nil) ->
          case flush(data) {
            Error(_) -> go_disconnected(data)
            Ok(Nil) -> {
              let reset = Data(..data, attempt: 0)
              maybe_throttle(reset)
            }
          }
      }
    // An attempt tick outside Connecting is stale; ignore it.
    Disconnected | Streaming | Throttled -> state_machine.keep_state(data, [])
  }
}

fn on_drain_tick(
  state: ConnectionState,
  data: Data,
) -> state_machine.Step(ConnectionState, Data, Message, reply) {
  case state {
    Throttled ->
      case flush(data) {
        Error(_) -> go_disconnected(data)
        Ok(Nil) -> maybe_throttle(data)
      }
    // A drain tick outside Throttled is stale; ignore it.
    Disconnected | Connecting | Streaming -> state_machine.keep_state(data, [])
  }
}

fn on_flush_tick(
  state: ConnectionState,
  data: Data,
) -> state_machine.Step(ConnectionState, Data, Message, reply) {
  case state {
    Streaming ->
      case flush(data) {
        Error(_) -> go_disconnected(data)
        // Re-arm the periodic flush (a state timeout fires only once).
        Ok(Nil) ->
          state_machine.keep_state(data, [
            StateTimeout(After(data.config.flush_interval_ms), FlushTick),
          ])
      }
    // A flush tick outside Streaming is stale; ignore it.
    Disconnected | Connecting | Throttled -> state_machine.keep_state(data, [])
  }
}

/// After a successful flush, decide between Streaming and Throttled based on
/// how much backlog remains relative to `max_pending`.
fn maybe_throttle(
  data: Data,
) -> state_machine.Step(ConnectionState, Data, Message, reply) {
  case hot_buffer.size(data.buffer) > data.config.max_pending {
    True ->
      state_machine.next_state(Throttled, data, [
        StateTimeout(After(data.config.base_ms), DrainTick),
      ])
    // Arm the periodic flush so buffer-only pushes get shipped.
    False ->
      state_machine.next_state(Streaming, data, [
        StateTimeout(After(data.config.flush_interval_ms), FlushTick),
      ])
  }
}

/// Transition to Disconnected and arm the jittered backoff retry timer,
/// bumping the attempt counter.
fn go_disconnected(
  data: Data,
) -> state_machine.Step(ConnectionState, Data, Message, reply) {
  let bumped = Data(..data, attempt: data.attempt + 1)
  let delay = jittered_delay(data.config, data.attempt)
  state_machine.next_state(Disconnected, bumped, [
    StateTimeout(After(delay), Tick),
  ])
}

// ---------------------------------------------------------------------------
// Flushing
// ---------------------------------------------------------------------------

/// Replay the cold (disk) tier first, then drain and deliver the hot (RAM)
/// tier. On the first delivery failure nothing is lost: cold-tier failures
/// re-spill the unsent remainder to disk, hot-tier failures push the remainder
/// back onto the ring. Either way an error is returned and the caller drops
/// back to Disconnected.
fn flush(data: Data) -> Result(Nil, String) {
  case replay_cold(data) {
    Error(reason) -> Error(reason)
    Ok(Nil) -> deliver_all(data, hot_buffer.drain(data.buffer))
  }
}

/// Drain the cold tier (oldest-first) and deliver it. A no-op when no cold tier
/// is configured. On a delivery failure the failed metric and all remaining
/// cold-tier metrics are re-spilled to disk (oldest-first) so disk stays first
/// on the next replay, and an error is returned.
fn replay_cold(data: Data) -> Result(Nil, String) {
  case data.spillover {
    None -> Ok(Nil)
    Some(sp) -> deliver_cold(data, sp, spillover.drain(sp))
  }
}

fn deliver_cold(
  data: Data,
  sp: Spillover,
  metrics: List(Metric),
) -> Result(Nil, String) {
  case metrics {
    [] -> Ok(Nil)
    [metric, ..rest] ->
      case data.transport.deliver(metric) {
        Ok(Nil) -> deliver_cold(data, sp, rest)
        Error(reason) -> {
          spillover.push_all(sp, [metric, ..rest])
          Error(reason)
        }
      }
  }
}

fn deliver_all(data: Data, metrics: List(Metric)) -> Result(Nil, String) {
  case metrics {
    [] -> Ok(Nil)
    [metric, ..rest] ->
      case data.transport.deliver(metric) {
        Ok(Nil) -> deliver_all(data, rest)
        Error(reason) -> {
          list.each([metric, ..rest], fn(m) {
            let _ = hot_buffer.push(data.buffer, m)
          })
          Error(reason)
        }
      }
  }
}

// ---------------------------------------------------------------------------
// Backoff
// ---------------------------------------------------------------------------

/// Deterministic exponential backoff: `min(max_ms, base_ms * 2^attempt)`.
/// `2^exponent` is an exact left shift (`1 << exponent`); the exponent is
/// capped at 16 so the shift can't run away.
pub fn backoff_delay(config: ConnectionConfig, attempt: Int) -> Int {
  let exponent = int.min(attempt, 16)
  let raw = config.base_ms * int.bitwise_shift_left(1, exponent)
  int.min(raw, config.max_ms)
}

/// "Full Jitter" backoff (AWS *Exponential Backoff And Jitter*): a uniform
/// random delay in `[0, backoff_delay(config, attempt))`. Spreading the delay
/// across the *whole* backoff window (rather than adding a small fixed jitter
/// band) minimises reconnection contention across a fleet.
pub fn jittered_delay(config: ConnectionConfig, attempt: Int) -> Int {
  int.random(backoff_delay(config, attempt))
}

// ---------------------------------------------------------------------------
// Brain transport (BEAM-to-BEAM ETF)
// ---------------------------------------------------------------------------

/// An opaque, resolved Brain destination (node atom + registered-name atom).
/// Built once by `resolve_brain_target`, which is where the node/name strings
/// are turned into atoms, so message volume never drives atom creation
/// (secure coding rule DSG-003). Reused by both the metric stream and the
/// Brain alert sink.
pub type BrainTarget

/// Resolve a Brain `node`/`name` config pair to a target, once. An empty
/// `node` targets the local node.
@external(erlang, "pharos_ffi", "brain_target")
pub fn resolve_brain_target(node: String, name: String) -> BrainTarget

/// Verify the target node is reachable.
@external(erlang, "pharos_ffi", "brain_connect")
pub fn brain_connect(target: BrainTarget) -> Result(Nil, String)

/// Send an already-ETF-encoded payload to the target's registered process.
@external(erlang, "pharos_ffi", "brain_deliver")
pub fn brain_deliver(
  target: BrainTarget,
  payload: BitArray,
) -> Result(Nil, String)

/// ETF-encode any term to a binary (`erlang:term_to_binary/1`).
@external(erlang, "pharos_ffi", "encode_etf")
pub fn encode_etf(term: a) -> BitArray

/// The Brain transport: connect verifies the remote node is reachable;
/// deliver ETF-encodes the metric and sends it to `{name, node}`.
/// The target's atoms are resolved once here, not per delivery.
pub fn brain_transport(node: String, name: String) -> Transport {
  let target = resolve_brain_target(node, name)
  Transport(connect: fn() { brain_connect(target) }, deliver: fn(metric) {
    brain_deliver(target, encode_etf(metric))
  })
}
