//// The cold tier of the spillover buffer.
////
//// An unbounded, disk-backed store of metrics held in a Dets file. The file is
//// opened and owned by a supervised actor: if the owner dies the file is
//// closed (and recovered by Dets's repair-on-open next time), and the
//// supervisor restarts the owner, which reopens the same file under the same
//// deterministic table name, so any rows written before the crash survive.
//// Readers and writers address the table by its atom name (resolved on every
//// call), so handles stay valid across owner restarts.
////
//// The connection manager spills into this tier when the Brain connection is
//// lost and the hot (ETS) buffer fills, and drains it first (oldest data) on
//// reconnect. See `pharos/internal/connection`.

import gleam/erlang/atom.{type Atom}
import gleam/erlang/process.{type Name}
import gleam/otp/actor
import gleam/otp/supervision.{type ChildSpecification}
import pharos/metric.{type Metric}

/// Restart-resilient handle to a spillover store. Wraps the Dets table's atom
/// name, which is deterministic and so stays valid across owner restarts.
pub opaque type Spillover {
  Spillover(table: Atom)
}

/// Wrap an existing table name as a handle. Used by readers/writers that know
/// the deterministic table name but not the owner pid.
pub fn from_table(table: Atom) -> Spillover {
  Spillover(table: table)
}

/// Use the atom underlying a `process.Name` as the Dets table name. `new_name`
/// already minted that atom (one per named process, as everywhere else here),
/// so no additional atoms are created for the buffer's table.
@external(erlang, "pharos_ffi", "name_to_atom")
pub fn table_name(name: Name(message)) -> Atom

/// The store's backing table name.
pub fn table(spillover: Spillover) -> Atom {
  spillover.table
}

/// A supervised worker that owns a spillover store's Dets file.
///
/// `table` is the Dets table name readers/writers use; `path` is the on-disk
/// file location. The owner actor itself receives no messages, its only job is
/// to hold the file open so its lifetime tracks the supervisor's.
pub fn child_spec(table: Atom, path: String) -> ChildSpecification(Spillover) {
  supervision.worker(fn() { start_link(table, path) })
}

/// Start a standalone spillover owner linked to the caller. Mainly used
/// directly in tests; in production the store is started via `child_spec`
/// under the pharos supervisor.
pub fn start_link(
  table: Atom,
  path: String,
) -> Result(actor.Started(Spillover), actor.StartError) {
  actor.new_with_initialiser(1000, fn(_subject) {
    open(table, path)
    actor.initialised(Spillover(table: table))
    |> actor.returning(Spillover(table: table))
    |> Ok
  })
  |> actor.start
}

/// Append a single metric to the cold store.
pub fn push(spillover: Spillover, metric: Metric) -> Nil {
  do_push(spillover.table, metric)
}

/// Append every metric (oldest-first) to the cold store. Used to bulk-spill a
/// hot-buffer drain when the ring fills during a partition.
pub fn push_all(spillover: Spillover, metrics: List(Metric)) -> Nil {
  do_push_all(spillover.table, metrics)
}

/// Return every stored metric oldest-first and clear the store. Used by the
/// connection manager to replay the cold tier before the hot tier on reconnect.
pub fn drain(spillover: Spillover) -> List(Metric) {
  do_drain(spillover.table)
}

/// Number of metrics currently stored on disk.
pub fn size(spillover: Spillover) -> Int {
  do_size(spillover.table)
}

// ---------------------------------------------------------------------------
// FFI
// ---------------------------------------------------------------------------

@external(erlang, "pharos_ffi", "spillover_open")
fn open(table: Atom, path: String) -> Nil

@external(erlang, "pharos_ffi", "spillover_push")
fn do_push(table: Atom, metric: Metric) -> Nil

@external(erlang, "pharos_ffi", "spillover_push_all")
fn do_push_all(table: Atom, metrics: List(Metric)) -> Nil

@external(erlang, "pharos_ffi", "spillover_drain")
fn do_drain(table: Atom) -> List(Metric)

@external(erlang, "pharos_ffi", "spillover_size")
fn do_size(table: Atom) -> Int
