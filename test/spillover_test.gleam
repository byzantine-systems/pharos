//// Spillover (Dets cold tier)

import gleam/erlang/atom
import gleam/int
import gleam/list
import gleeunit/should
import pharos/internal/clock
import pharos/internal/spillover
import pharos/metric

fn sample_metric(name: String) -> metric.Metric {
  metric.new(name, 1.0)
}

/// A table name + on-disk path unique to this run, so test files never collide
/// or carry stale rows across runs. `build/` exists during `gleam test`.
fn unique(base: String) -> #(atom.Atom, String) {
  let name = base <> "_" <> int.to_string(clock.now_ms())
  #(atom.create(name), "build/" <> name <> ".dets")
}

pub fn spillover_push_drain_oldest_first_test() -> Nil {
  let #(table, path) = unique("pharos_test_spill_ppd")
  let assert Ok(started) = spillover.start_link(table, path)
  let store = started.data

  spillover.size(store) |> should.equal(0)
  // drain on an empty store yields the empty list.
  spillover.drain(store) |> should.equal([])

  spillover.push(store, sample_metric("a"))
  spillover.push_all(store, [sample_metric("b"), sample_metric("c")])
  spillover.size(store) |> should.equal(3)

  // drain returns oldest-first across both push and push_all, and clears.
  list.map(spillover.drain(store), fn(m) { m.name })
  |> should.equal(["a", "b", "c"])
  spillover.size(store) |> should.equal(0)
}
