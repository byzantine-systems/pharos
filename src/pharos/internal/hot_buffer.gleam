//// The hot tier of the spillover buffer.
////
//// A bounded, in-memory **circular buffer** of recent `Metric`s held in a
//// public named ETS table. The table is created and owned by a supervised
//// actor: if the owner dies the table dies with it, and the supervisor
//// restarts the owner, which recreates an empty table under the same
//// deterministic name. Readers and writers address the table by its atom
//// name (resolved on every call), so handles survive owner restarts.
////
//// Writes go straight to ETS from the calling process (e.g. the telemetry
//// handler) for near-zero latency rather than serialising through the owner.
//// When a push exceeds `capacity` the oldest entry is evicted and the push
//// reports an overflow, which the connection manager can use
//// to decide when to spill to the cold (Dets) tier.

import gleam/erlang/atom.{type Atom}
import gleam/erlang/process.{type Name}
import gleam/otp/actor
import gleam/otp/supervision.{type ChildSpecification}
import pharos/metric.{type Metric}

/// Restart-resilient handle to a hot buffer. Wraps the ETS table's atom name,
/// which is deterministic and so stays valid across owner restarts.
pub opaque type HotBuffer {
  HotBuffer(table: Atom)
}

/// Wrap an existing table name as a handle. Used by readers/writers that know
/// the deterministic table name but not the owner pid.
pub fn from_table(table: Atom) -> HotBuffer {
  HotBuffer(table: table)
}

/// Use the atom underlying a `process.Name` as the ETS table name. `new_name`
/// already minted that atom (one per named process, as everywhere else here),
/// so no additional atoms are created for the buffer's table.
@external(erlang, "pharos_ffi", "name_to_atom")
pub fn table_name(name: Name(message)) -> Atom

/// The buffer's backing table name.
pub fn table(buffer: HotBuffer) -> Atom {
  buffer.table
}

/// A supervised worker that owns a hot buffer's ETS table.
///
/// `table` is the public ETS table name readers/writers use; `capacity` bounds
/// the ring before eviction kicks in. The owner actor itself receives no
/// messages, its only job is to own the table so its lifetime tracks the
/// supervisor's. Readers address the table by its (stable) atom name, so the
/// owner needs no registered name of its own.
pub fn child_spec(table: Atom, capacity: Int) -> ChildSpecification(HotBuffer) {
  supervision.worker(fn() { start_link(table, capacity) })
}

/// Start a standalone hot-buffer owner linked to the caller. Mainly used
/// directly in tests; in production the buffer is started via `child_spec`
/// under the pharos supervisor.
pub fn start_link(
  table: Atom,
  capacity: Int,
) -> Result(actor.Started(HotBuffer), actor.StartError) {
  actor.new_with_initialiser(1000, fn(_subject) {
    init_table(table, capacity)
    actor.initialised(HotBuffer(table: table))
    |> actor.returning(HotBuffer(table: table))
    |> Ok
  })
  |> actor.start
}

/// Append `metric`, evicting the oldest entry if the buffer is full. Returns
/// `True` when an eviction occurred (the buffer was at capacity).
pub fn push(buffer: HotBuffer, metric: Metric) -> Bool {
  do_push(buffer.table, metric)
}

/// Return every buffered metric oldest-first and clear the buffer. Used by the
/// connection manager when flushing on (re)connect.
pub fn drain(buffer: HotBuffer) -> List(Metric) {
  do_drain(buffer.table)
}

/// Return every buffered metric oldest-first without clearing.
pub fn peek(buffer: HotBuffer) -> List(Metric) {
  do_peek(buffer.table)
}

/// Number of metrics currently buffered.
pub fn size(buffer: HotBuffer) -> Int {
  do_size(buffer.table)
}

// ---------------------------------------------------------------------------
// FFI
// ---------------------------------------------------------------------------

@external(erlang, "pharos_ffi", "hot_buffer_init")
fn init_table(table: Atom, capacity: Int) -> Nil

@external(erlang, "pharos_ffi", "hot_buffer_push")
fn do_push(table: Atom, metric: Metric) -> Bool

@external(erlang, "pharos_ffi", "hot_buffer_drain")
fn do_drain(table: Atom) -> List(Metric)

@external(erlang, "pharos_ffi", "hot_buffer_peek")
fn do_peek(table: Atom) -> List(Metric)

@external(erlang, "pharos_ffi", "hot_buffer_size")
fn do_size(table: Atom) -> Int
