import gleam/int
import gleeunit
import gleeunit/should
import pharos
import pharos/alert
import pharos/config
import pharos/internal/clock
import pharos/statistic

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn start_link_returns_ok_test() -> Nil {
  let assert Ok(started) =
    pharos.start_link(
      config.new()
      |> config.with_statistics([statistic.poll(statistic.BeamMemory)]),
    )
  pharos.stop(started.data)
}

pub fn start_link_with_no_statistics_succeeds_test() -> Nil {
  let assert Ok(started) = pharos.start_link(config.new())
  pharos.stop(started.data)
}

pub fn start_link_with_windowed_threshold_succeeds_test() -> Nil {
  // A windowed threshold wires up a windowed alert_manager and starts cleanly.
  let assert Ok(started) =
    pharos.start_link(
      config.new()
      |> config.with_statistics([statistic.poll(statistic.HostCpu)])
      |> config.with_thresholds([
        config.Windowed(
          config.HostCpuUtil(above: 80.0),
          2000,
          alert.Percentage(at_least: 0.9),
        ),
      ]),
    )
  pharos.stop(started.data)
}

pub fn start_link_with_poll_jitter_succeeds_test() -> Nil {
  // Jittered pollers start cleanly (init_delay only shifts the first tick).
  let assert Ok(started) =
    pharos.start_link(
      config.new()
      |> config.with_statistics([statistic.poll(statistic.BeamMemory)])
      |> config.with_poll_jitter(0.2),
    )
  pharos.stop(started.data)
}

pub fn subscribe_returns_handler_id_test() -> Nil {
  let assert Ok(started) = pharos.start_link(config.new())
  pharos.subscribe(started.data, fn(_event) { Nil })
  |> should.be_ok
  pharos.stop(started.data)
}

pub fn start_link_with_brain_and_spillover_succeeds_test() -> Nil {
  // Brain + spillover path: the cold-tier owner child starts under the
  // supervisor and the connection manager shares its handle.
  let path = "build/pharos_test_start_spill_" <> int.to_string(clock.now_ms())
  let assert Ok(started) =
    pharos.start_link(
      config.new()
      |> config.with_brain_stream("", "pharos_test_brain")
      |> config.with_metric_spillover(path),
    )
  pharos.stop(started.data)
}

pub fn start_link_with_brain_no_spillover_stays_ram_only_test() -> Nil {
  // Brain but no spillover path: starts cleanly in RAM-only mode (no cold tier).
  let assert Ok(started) =
    pharos.start_link(
      config.new()
      |> config.with_brain_stream("", "pharos_test_brain"),
    )
  pharos.stop(started.data)
}
