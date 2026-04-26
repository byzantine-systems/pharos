import gleeunit
import gleeunit/should
import pharos
import pharos/config
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

pub fn subscribe_returns_handler_id_test() -> Nil {
  let assert Ok(started) = pharos.start_link(config.new())
  pharos.subscribe(started.data, fn(_event) { Nil })
  |> should.be_ok
  pharos.stop(started.data)
}
