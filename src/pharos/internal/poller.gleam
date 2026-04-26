//// Telemetry poller wrapping.
////
//// Translates the user's `List(Statistic)` into one or more
//// `telemetry_poller` instances, bucketed by polling interval. Each bucket
//// becomes one supervised child of the pharos supervisor: every poller
//// owns the list of measurements that share its interval.
////
//// Built-in measurements (`BeamMemory`, run queues, system counts,
//// persistent term) lower to `telemetry_poller`'s built-in measurements.
//// Custom measurements (`ProcessInfo`, `ClusterNodes`, `HostMemory`,
//// `HostDisk`) lower to either `telemetry_poller`'s `process_info` helper
//// or to a `{module, function, args}` MFA pointing at one of pharos_ffi's
//// emitters.

import gleam/dict
import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom.{type Atom}
import gleam/erlang/process.{type Pid}
import gleam/float
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision.{type ChildSpecification}
import pharos/probe.{type Probe}
import pharos/statistic.{
  type Statistic, type StatisticKind, BeamMemory, BeamPersistentTerm,
  BeamReductions, BeamRunQueues, BeamScheduler, BeamSystemCounts, ClusterNodes,
  HostCpu, HostDisk, HostMemory, HostNetwork, ProcessInfo, Statistic,
}

// ---------------------------------------------------------------------------
// Typed errors
// ---------------------------------------------------------------------------

pub type PollerStartError {
  PollerIgnored
  PollerStartFailed(reason: String)
}

// ---------------------------------------------------------------------------
// telemetry_poller raw start option type. Mirrors `option/0` in
// telemetry_poller's Erlang API; lowered to a keyword list at the FFI
// boundary.
// ---------------------------------------------------------------------------

type StartOption {
  Period(Int)
  /// Delay (ms) before the first measurement; used to jitter the poller's
  /// schedule. Lowered to telemetry_poller's `{init_delay, _}`.
  InitDelay(Int)
  Measurements(List(PollerMeasurement))
}

type PollerMeasurement {
  /// Built-in measurement: lowered to a bare atom (`memory`, etc.) by
  /// telemetry_poller.
  Builtin(name: Atom)
  /// `process_info` measurement: lowered to
  /// `{process_info, [{name, _}, {event, _}, {keys, _}]}`.
  ProcessInfoSpec(name: Atom, event: List(Atom), keys: List(Atom))
  /// Custom measurement: lowered to `{module, function, args}` MFA. The
  /// function is called by telemetry_poller every polling cycle and is
  /// expected to call `telemetry:execute/3` itself.
  Custom(module: Atom, function: Atom, args: List(Dynamic))
}

// ---------------------------------------------------------------------------
// Public: build child specs for the supervisor
// ---------------------------------------------------------------------------

/// Group `statistics` by polling interval and return one supervised poller
/// child per distinct interval. `jitter` is the optional polling-jitter ratio
/// (see `config.poll_jitter`). Returns an empty list when `statistics` is
/// empty.
pub fn child_specs(
  statistics: List(Statistic),
  jitter: Option(Float),
) -> List(ChildSpecification(Pid)) {
  bucket_by_interval(statistics)
  |> list.map(fn(bucket) {
    let #(interval_ms, kinds) = bucket
    poller_child(interval_ms, list.map(kinds, lower_kind), jitter)
  })
}

/// Group `probes` by polling interval and return one supervised poller child
/// per distinct interval. Each probe lowers to a `Custom` MFA measurement;
/// the emitter calls `telemetry:execute/3` itself. Returns an empty list when
/// `probes` is empty.
pub fn probe_child_specs(
  probes: List(Probe),
  jitter: Option(Float),
) -> List(ChildSpecification(Pid)) {
  bucket_probes_by_interval(probes)
  |> list.map(fn(bucket) {
    let #(interval_ms, measurements) = bucket
    poller_child(interval_ms, measurements, jitter)
  })
}

fn poller_child(
  interval_ms: Int,
  measurements: List(PollerMeasurement),
  jitter: Option(Float),
) -> ChildSpecification(Pid) {
  supervision.worker(fn() {
    // A fresh random init_delay is drawn per (re)start, so a poller crash
    // re-phases it and a fleet restarting together still de-correlates.
    let options =
      [Period(interval_ms), Measurements(measurements)]
      |> list.append(init_delay_options(interval_ms, jitter))
    case start_poller(options) {
      Ok(pid) -> Ok(actor.Started(pid: pid, data: pid))
      Error(PollerIgnored) ->
        Error(actor.InitFailed("telemetry_poller ignored startup"))
      Error(PollerStartFailed(reason: reason)) ->
        Error(actor.InitFailed(reason))
    }
  })
}

/// The `InitDelay` option for a poller, or none. A random delay in
/// `[0, jitter_spread)` is chosen when jitter is enabled and the spread is
/// positive; otherwise no option is emitted (telemetry_poller defaults to 0).
fn init_delay_options(
  interval_ms: Int,
  jitter: Option(Float),
) -> List(StartOption) {
  case jitter {
    None -> []
    Some(ratio) ->
      case jitter_spread(interval_ms, ratio) {
        0 -> []
        // `int.random(spread)` is uniform in `[0, spread)`.
        spread -> [InitDelay(int.random(spread))]
      }
  }
}

/// Maximum jitter delay (ms) for an interval: `round(interval_ms * ratio)`,
/// never negative. Pure, so the bound is unit-testable independently of the
/// random draw.
pub fn jitter_spread(interval_ms: Int, ratio: Float) -> Int {
  int.max(float.round(int.to_float(interval_ms) *. ratio), 0)
}

// ---------------------------------------------------------------------------
// Bucketing
// ---------------------------------------------------------------------------

fn bucket_by_interval(
  statistics: List(Statistic),
) -> List(#(Int, List(StatisticKind))) {
  let grouped =
    list.fold(statistics, dict.new(), fn(acc, stat) {
      let Statistic(kind:, interval_ms:) = stat
      let existing = case dict.get(acc, interval_ms) {
        Ok(kinds) -> kinds
        Error(Nil) -> []
      }
      dict.insert(acc, interval_ms, [kind, ..existing])
    })
  dict.to_list(grouped)
}

fn bucket_probes_by_interval(
  probes: List(Probe),
) -> List(#(Int, List(PollerMeasurement))) {
  let grouped =
    list.fold(probes, dict.new(), fn(acc, definition) {
      let interval_ms = probe.interval_ms(definition)
      let existing = case dict.get(acc, interval_ms) {
        Ok(measurements) -> measurements
        Error(Nil) -> []
      }
      dict.insert(acc, interval_ms, [lower_probe(definition), ..existing])
    })
  dict.to_list(grouped)
}

// ---------------------------------------------------------------------------
// Lowering Statistic kinds to telemetry_poller measurements
// ---------------------------------------------------------------------------

fn lower_probe(definition: Probe) -> PollerMeasurement {
  case probe.source(definition) {
    probe.CustomMfa(module:, function:, args:) ->
      Custom(module: module, function: function, args: args)
  }
}

fn lower_kind(kind: StatisticKind) -> PollerMeasurement {
  case kind {
    BeamMemory -> Builtin(name: atom.create("memory"))
    BeamRunQueues -> Builtin(name: atom.create("total_run_queue_lengths"))
    BeamSystemCounts -> Builtin(name: atom.create("system_counts"))
    BeamPersistentTerm -> Builtin(name: atom.create("persistent_term"))
    ProcessInfo(name:, event:, keys:) ->
      ProcessInfoSpec(name: name, event: event, keys: keys)
    ClusterNodes ->
      Custom(
        module: atom.create("pharos_ffi"),
        function: atom.create("emit_cluster_nodes"),
        args: [],
      )
    HostMemory ->
      Custom(
        module: atom.create("pharos_ffi"),
        function: atom.create("emit_host_memory"),
        args: [],
      )
    HostDisk(path:) ->
      Custom(
        module: atom.create("pharos_ffi"),
        function: atom.create("emit_host_disk"),
        args: [dynamic.string(path)],
      )
    HostCpu ->
      Custom(
        module: atom.create("pharos_ffi"),
        function: atom.create("emit_host_cpu"),
        args: [],
      )
    HostNetwork ->
      Custom(
        module: atom.create("pharos_ffi"),
        function: atom.create("emit_host_network"),
        args: [],
      )
    BeamScheduler ->
      Custom(
        module: atom.create("pharos_ffi"),
        function: atom.create("emit_beam_scheduler"),
        args: [],
      )
    BeamReductions ->
      Custom(
        module: atom.create("pharos_ffi"),
        function: atom.create("emit_beam_reductions"),
        args: [],
      )
  }
}

// ---------------------------------------------------------------------------
// FFI
// ---------------------------------------------------------------------------

@external(erlang, "pharos_ffi", "start_poller")
fn start_poller(options: List(StartOption)) -> Result(Pid, PollerStartError)
