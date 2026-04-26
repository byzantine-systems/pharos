//// Poller bucketing and polling jitter.

import gleam/list
import gleam/option.{None, Some}
import gleeunit/should
import pharos/internal/poller
import pharos/statistic

pub fn jitter_spread_scales_with_ratio_test() -> Nil {
  poller.jitter_spread(10_000, 0.1) |> should.equal(1000)
  poller.jitter_spread(1000, 0.1) |> should.equal(100)
}

pub fn jitter_spread_zero_ratio_is_zero_test() -> Nil {
  poller.jitter_spread(10_000, 0.0) |> should.equal(0)
}

pub fn jitter_spread_rounds_test() -> Nil {
  poller.jitter_spread(1004, 0.1) |> should.equal(100)
  poller.jitter_spread(1006, 0.1) |> should.equal(101)
}

pub fn jitter_spread_never_negative_test() -> Nil {
  // Floored at 0 even for a nonsensical negative ratio.
  poller.jitter_spread(10_000, -0.5) |> should.equal(0)
}

pub fn child_specs_one_per_interval_test() -> Nil {
  // Two distinct intervals (BeamMemory default 5000, overridden 500) -> 2 pollers.
  let stats = [
    statistic.poll(statistic.BeamMemory),
    statistic.poll_every(statistic.BeamRunQueues, 500),
  ]
  poller.child_specs(stats, Some(0.1)) |> list.length |> should.equal(2)
  // Jitter setting doesn't change the number of pollers.
  poller.child_specs(stats, None) |> list.length |> should.equal(2)
}
