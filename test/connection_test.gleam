//// Connection manager

import gleam/erlang/atom
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleeunit/should
import pharos/internal/clock
import pharos/internal/connection
import pharos/internal/hot_buffer
import pharos/internal/spillover
import pharos/metric

fn test_conn_config() -> connection.ConnectionConfig {
  connection.ConnectionConfig(
    base_ms: 50,
    max_ms: 200,
    max_pending: 1000,
    flush_interval_ms: 50,
  )
}

pub fn backoff_delay_caps_at_max_test() -> Nil {
  let config =
    connection.ConnectionConfig(
      base_ms: 100,
      max_ms: 1000,
      max_pending: 10,
      flush_interval_ms: 100,
    )
  connection.backoff_delay(config, 0) |> should.equal(100)
  connection.backoff_delay(config, 1) |> should.equal(200)
  connection.backoff_delay(config, 3) |> should.equal(800)
  // 100 * 2^4 = 1600, capped to max_ms.
  connection.backoff_delay(config, 4) |> should.equal(1000)
  connection.backoff_delay(config, 100) |> should.equal(1000)
}

pub fn jittered_delay_within_bounds_test() -> Nil {
  let config =
    connection.ConnectionConfig(
      base_ms: 100,
      max_ms: 1000,
      max_pending: 10,
      flush_interval_ms: 100,
    )
  // Full Jitter: a uniform delay in `[0, backoff_delay(config, attempt))`.
  let delay = connection.jittered_delay(config, 0)
  { delay >= 0 } |> should.be_true
  { delay < connection.backoff_delay(config, 0) } |> should.be_true

  // Higher attempt: still within `[0, capped backoff)`.
  let later = connection.jittered_delay(config, 3)
  { later >= 0 } |> should.be_true
  { later < connection.backoff_delay(config, 3) } |> should.be_true
}

pub fn connection_streams_buffered_metrics_test() -> Nil {
  let probe = process.new_subject()
  let assert Ok(buffer_started) =
    hot_buffer.start_link(atom.create("pharos_test_hb_conn_ok"), 100)
  let transport =
    connection.Transport(connect: fn() { Ok(Nil) }, deliver: fn(metric) {
      process.send(probe, metric.name)
      Ok(Nil)
    })
  let assert Ok(conn) =
    connection.start_link(
      process.new_name("pharos_test_conn_ok"),
      transport,
      buffer_started.data,
      None,
      test_conn_config(),
    )
  connection.deliver(conn, metric.new("vm.memory.total", 1.0))
  process.receive(probe, 1000) |> should.equal(Ok("vm.memory.total"))
}

pub fn connection_buffers_while_disconnected_test() -> Nil {
  let probe = process.new_subject()
  let assert Ok(buffer_started) =
    hot_buffer.start_link(atom.create("pharos_test_hb_conn_down"), 100)
  let buffer = buffer_started.data
  let transport =
    connection.Transport(connect: fn() { Error("down") }, deliver: fn(metric) {
      process.send(probe, metric.name)
      Ok(Nil)
    })
  let assert Ok(conn) =
    connection.start_link(
      process.new_name("pharos_test_conn_down"),
      transport,
      buffer,
      None,
      test_conn_config(),
    )
  connection.deliver(conn, metric.new("x", 1.0))
  // Never connects, so nothing is delivered, but the metric is buffered.
  process.receive(probe, 300) |> should.equal(Error(Nil))
  { hot_buffer.size(buffer) >= 1 } |> should.be_true
}

fn unique_dets(base: String) -> #(atom.Atom, String) {
  let name = base <> "_" <> int.to_string(clock.now_ms())
  #(atom.create(name), "build/" <> name <> ".dets")
}

fn collect(probe: process.Subject(String), n: Int) -> List(String) {
  case n {
    0 -> []
    _ ->
      case process.receive(probe, 2000) {
        Ok(name) -> [name, ..collect(probe, n - 1)]
        Error(_) -> []
      }
  }
}

pub fn connection_replays_disk_then_ram_oldest_first_test() -> Nil {
  let probe = process.new_subject()
  let assert Ok(buffer_started) =
    hot_buffer.start_link(atom.create("pharos_test_hb_replay"), 1000)
  let buffer = buffer_started.data
  let #(table, path) = unique_dets("pharos_test_spill_replay")
  let assert Ok(spill_started) = spillover.start_link(table, path)
  let store = spill_started.data

  // Connect only succeeds once something has spilled to disk: this forces a
  // buffering phase long enough to exercise the cold tier before reconnect.
  let transport =
    connection.Transport(
      connect: fn() {
        case spillover.size(store) > 0 {
          True -> Ok(Nil)
          False -> Error("warming up")
        }
      },
      deliver: fn(metric) {
        process.send(probe, metric.name)
        Ok(Nil)
      },
    )
  // base_ms is comfortably larger than the time to enqueue the five casts, so
  // the metrics buffer (and spill) before the first successful retry fires.
  // max_pending: 2 makes the ring spill to disk early.
  let config =
    connection.ConnectionConfig(
      base_ms: 300,
      max_ms: 300,
      max_pending: 2,
      flush_interval_ms: 50,
    )
  let assert Ok(conn) =
    connection.start_link(
      process.new_name("pharos_test_conn_replay"),
      transport,
      buffer,
      Some(store),
      config,
    )

  list.each(["a", "b", "c", "d", "e"], fn(name) {
    connection.deliver(conn, metric.new(name, 1.0))
  })

  // On reconnect the cold tier replays first, then the hot tier, so the "Brain"
  // sees every metric exactly once, oldest-first, none lost to ring eviction.
  collect(probe, 5) |> should.equal(["a", "b", "c", "d", "e"])
}
