# Pharos: Usage Guide & Architecture

Pharos is a producer-only "smart sensor" for the BEAM. One instance runs per node/host. It
**collects** signals, **evaluates** thresholds locally into alerts, and **streams** raw
metrics to a central collector ("the Brain"), staying alive (and lossless) through network
partitions.

This guide covers the moving parts. For the API reference see <https://hexdocs.pm/pharos>;
for a copy-paste starting point see [Quick_Start.md](Quick_Start.md).

## Pipeline at a glance

```
                         ┌──────────── pharos.start_link(config) ────────────┐
                         │                                                   │
  pollers ──telemetry──▶ │  decode ─▶ threshold eval ─▶ soak/cool FSM ─▶ event bus ─▶ sinks
  (per interval bucket)  │     │                          (per threshold)        (console / webhook / Brain)
                         │     └─▶ metric flatten ─▶ hot buffer (ETS) ─▶ connection mgr ─▶ Brain
                         │                                  ▲                    (ETF, backoff+jitter)
                         │                          spillover (Dets) ◀──spill / replay──┘
                         └───────────────────────────────────────────────────┘
```

Two lanes share the collection front-end:

- **Alerting lane**: decoded measurements are checked against thresholds; a per-threshold
  state machine debounces and emits `AlertFiring` / `AlertResolved` on the event bus, where
  configured **sinks** receive them.
- **Metric lane**: the same measurements are flattened into transport-ready `Metric`s,
  buffered, and streamed to the Brain by a resilient connection manager.

Everything is started as one supervised tree; you embed it via `pharos.start_link/1` and hold
the returned handle.

## Collection: what you can poll

A `Statistic` pairs a `StatisticKind` with a polling interval. Use `statistic.poll(kind)` for
the default cadence or `statistic.poll_every(kind, ms)` to override it.

| Kind | Event | Source | Notes |
|------|-------|--------|-------|
| `BeamMemory` | `[vm, memory]` | `erlang:memory` | telemetry_poller builtin |
| `BeamRunQueues` | `[vm, total_run_queue_lengths]` | builtin | |
| `BeamSystemCounts` | `[vm, system_counts]` | builtin | process/atom/port counts + limits |
| `BeamPersistentTerm` | `[vm, persistent_term]` | builtin | |
| `BeamScheduler` | `[pharos, vm, scheduler]` | `scheduler_wall_time` | utilization %, delta-based |
| `BeamReductions` | `[pharos, vm, reductions]` | `statistics(reductions)` | reductions since last poll |
| `ProcessInfo(name, event, keys)` | custom | `process_info` | per-named-process snapshot |
| `ClusterNodes` | `[pharos, cluster, nodes]` | `erlang:nodes` | distributed topology |
| `HostMemory` | `[pharos, host, memory]` | `/proc/meminfo` | Linux |
| `HostDisk(path)` | `[pharos, host, disk]` | os_mon `disksup` | usage of the mount holding `path` |
| `HostCpu` | `[pharos, host, cpu]` | os_mon `cpu_sup` | utilization % + load averages |
| `HostNetwork` | `[pharos, host, network]` | `/proc/net/dev` | per-second rx/tx rates (non-`lo`) |

Memory values are scaled to `config.memory_unit` (`Kb`/`Mb`/`Gb`, default `Mb`). Host probes
degrade gracefully (`status: unimplemented`) when their source is unavailable, and such
samples never fire thresholds.

```gleam
config.new()
|> config.with_statistics([
  statistic.poll(statistic.BeamMemory),
  statistic.poll_every(statistic.BeamRunQueues, 500),
  statistic.poll(statistic.HostCpu),
  statistic.poll(statistic.HostDisk("/")),
])
```

## Thresholds & the alerting engine

Each `config.Threshold` maps 1:1 to a measured field and is `above`-comparing:

| Group | Thresholds |
|-------|-----------|
| BEAM memory | `TotalMemory` · `ProcessMemory` · `SystemMemory` · `BinaryMemory` · `EtsMemory` |
| Run queues | `TotalRunQueue` · `CpuRunQueue` |
| System counts | `ProcessCount` · `AtomCount` · `PortCount` |
| Persistent terms | `PersistentTermCount` · `PersistentTermMemory` |
| Host / scheduler | `HostMemoryUsed` · `HostDiskUsed` · `HostCpuUtil` · `SchedulerUtilization` |

A threshold doesn't fire the instant it is breached. Each threshold gets its own state machine
(an `eparch` `state_machine`) that applies a **soak** period (the breach must hold for
`soak_period_ms`, default 30 s) before emitting `AlertFiring`, and a **cool** period
(recovery must hold for `cool_period_ms`, default 60 s) before emitting `AlertResolved`. This
debounces flapping and de-duplicates: one breach → one alert → one resolution.

```gleam
config.new()
|> config.with_thresholds([
  config.TotalMemory(above: 500.0),       // Mb
  config.HostCpuUtil(above: 85.0),        // percent
  config.HostDiskUsed(above: 50_000.0),   // Mb on the monitored mount
])
|> config.with_soak_period(15_000)
|> config.with_cool_period(30_000)
|> config.with_default_alert_level(alert.Critical)
```

Subscribe to the resulting alert stream:

```gleam
let assert Ok(handler) =
  pharos.subscribe(started.data, fn(event) {
    case event {
      alert.AlertFiring(id:, level:, diagnostic:) -> ...
      alert.AlertResolved(id:) -> ...
    }
  })
// pharos.unsubscribe(started.data, handler)
```

## Alert sinks

Configured sinks each attach as an independent, fire-and-forget handler on the event bus, so
a slow or failing sink never blocks the others:

```gleam
import logging

config.new()
|> config.with_alert_sinks([
  config.Console(level: logging.Warning),          // logs locally (journald)
  config.Webhook(url: "https://hooks.example/..."),// JSON POST (e.g. Slack)
  config.Brain(node: "brain@10.0.0.1", name: "ingest"), // BEAM-to-BEAM ETF
  // config.Otlp(endpoint: "...")                  // documented stub for now
])
```

## Custom probes (the application tier)

Beyond the built-in kinds, the **probe lane** lets you monitor any `:telemetry` signal (or
emit your own) without modifying Pharos. A `Probe` carries an id, an event name, an interval,
a `Source` (an MFA emitter), and a decode function producing a `MetricSample`
(`Dict(String, Float)`). `ProbeThreshold`s compare a single sample field with `Above`/`Below`:

```gleam
config.new()
|> config.with_custom_statistics([my_probe])
|> config.with_custom_thresholds([
  probe.ProbeThreshold("vm", "free_mb", probe.Below(128.0), alert.Warning),
])
```

This is the integration point for **logothetes** to monitor MicroVMs via `/proc/<pid>` metrics
without Pharos depending on it.

## The resilient metric stream

Set a Brain target to enable the metric pipeline (without it, metrics are still collected and
buffered locally, just not shipped):

```gleam
config.new()
|> config.with_metric_buffer_capacity(10_000)             // hot ring size
|> config.with_brain_stream(node: "brain@10.0.0.1", name: "ingest")
|> config.with_metric_spillover("/var/lib/pharos/spill.dets") // optional cold tier
```

- **Hot buffer (ETS):** a bounded circular buffer of recent `Metric`s; near-zero-latency writes.
- **Connection manager (`gen_statem`):** `Disconnected → Connecting → Streaming → Throttled`,
  reconnecting with **exponential backoff + jitter** so a fleet never reconnects in lockstep.
- **Spillover (Dets):** when the connection is down and the hot buffer fills, metrics spill to
  a disk file instead of being evicted. On reconnect the agent **replays disk first, then RAM,
  then resumes real-time**, so a partition loses nothing.

The cold tier is opt-in (`with_metric_spillover`) and only active alongside a Brain stream.

## Configuration reference

| Setter | Default | Purpose |
|--------|---------|---------|
| `with_statistics` | `[]` | built-in kinds to poll |
| `with_thresholds` | `[]` | alert thresholds (empty = observe only) |
| `with_custom_statistics` | `[]` | custom probes |
| `with_custom_thresholds` | `[]` | probe thresholds |
| `with_alert_sinks` | `[Console(Warning)]` | where alerts go |
| `with_memory_unit` | `Mb` | unit for memory metrics/thresholds |
| `with_soak_period` | `30_000` ms | breach must hold before firing |
| `with_cool_period` | `60_000` ms | recovery must hold before resolving |
| `with_default_alert_level` | `Warning` | severity on firing alerts |
| `with_metric_buffer_capacity` | `10_000` | hot-buffer ring size |
| `with_brain_stream` | `None` | enable the metric stream to the Brain |
| `with_metric_spillover` | `None` | enable the Dets cold tier |

## Lifecycle

`pharos.start_link/1` returns `Result(actor.Started(Pharos), StartError)`. Embed the returned
`pid` in your supervision tree (or hold `started.data`). Call `pharos.stop/1` from the owning
process to detach the telemetry handler and tear the tree down; it is idempotent.

## Status

Pharos is under active development. Implemented: three-tier collection (host
memory/disk/CPU/network + BEAM internals, scheduler, reductions), the local alerting engine,
the pluggable sink bus, and the hot/cold resilient stream with replay. On the roadmap:
systemd status via D-Bus, sliding-window threshold aggregation, alert inhibition, jittered
polling, an OTLP exporter, and a hardened NixOS module.
