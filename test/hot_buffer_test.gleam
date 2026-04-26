//// Hot buffer

import gleam/erlang/atom
import gleam/list
import gleeunit/should
import pharos/internal/hot_buffer
import pharos/metric

fn sample_metric(name: String) -> metric.Metric {
  metric.new(name, 1.0)
}

pub fn hot_buffer_push_peek_drain_test() -> Nil {
  let assert Ok(started) =
    hot_buffer.start_link(atom.create("pharos_test_hb_ppd"), 100)
  let buffer = started.data
  hot_buffer.size(buffer) |> should.equal(0)

  let _ = hot_buffer.push(buffer, sample_metric("a"))
  let _ = hot_buffer.push(buffer, sample_metric("b"))
  hot_buffer.size(buffer) |> should.equal(2)

  // peek is oldest-first and non-destructive.
  list.map(hot_buffer.peek(buffer), fn(m) { m.name })
  |> should.equal(["a", "b"])
  hot_buffer.size(buffer) |> should.equal(2)

  // drain returns the same order and clears.
  list.map(hot_buffer.drain(buffer), fn(m) { m.name })
  |> should.equal(["a", "b"])
  hot_buffer.size(buffer) |> should.equal(0)
}

pub fn hot_buffer_overflow_evicts_oldest_test() -> Nil {
  let assert Ok(started) =
    hot_buffer.start_link(atom.create("pharos_test_hb_overflow"), 2)
  let buffer = started.data
  let _ = hot_buffer.push(buffer, sample_metric("a"))
  let _ = hot_buffer.push(buffer, sample_metric("b"))
  // Third push exceeds capacity: reports overflow and evicts the oldest.
  hot_buffer.push(buffer, sample_metric("c")) |> should.be_true
  hot_buffer.size(buffer) |> should.equal(2)
  list.map(hot_buffer.peek(buffer), fn(m) { m.name })
  |> should.equal(["b", "c"])
}
