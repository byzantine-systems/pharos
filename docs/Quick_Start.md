# Quick Start

## Install

```sh
gleam add pharos
```

Pharos runs on the Erlang target (OTP 27+). Host disk/CPU collection uses OTP's `os_mon`.

## Monitor a node in three steps

```gleam
import gleam/io
import pharos
import pharos/alert
import pharos/config
import pharos/statistic

pub fn main() {
  // 1. Describe what to collect and when to alert.
  let configuration =
    config.new()
    |> config.with_statistics([
      statistic.poll(statistic.BeamMemory),
      statistic.poll_every(statistic.BeamRunQueues, 500),
      statistic.poll(statistic.HostCpu),
    ])
    |> config.with_thresholds([
      config.TotalMemory(above: 500.0),
      // Mb of total BEAM memory
      config.HostCpuUtil(above: 85.0),
      // host CPU percent
    ])

  // 2. Start the supervised tree.
  let assert Ok(started) = pharos.start_link(configuration)

  // 3. React to alerts.
  let assert Ok(_handler) =
    pharos.subscribe(started.data, fn(event) {
      case event {
        alert.AlertFiring(id:, level: _, diagnostic:) ->
          io.println("FIRING " <> id <> ": " <> diagnostic)
        alert.AlertResolved(id:) -> io.println("RESOLVED " <> id)
      }
    })
}
```

Thresholds debounce: a breach must hold for the **soak** period (default 30 s) before
`AlertFiring`, and recovery must hold for the **cool** period (default 60 s) before
`AlertResolved`. Tune with `config.with_soak_period/2` and `config.with_cool_period/2`.

## Send alerts somewhere

```gleam
import logging

config.new()
|> config.with_alert_sinks([
  config.Console(level: logging.Warning),
  config.Webhook(url: "https://hooks.example/services/XXX"),
])
```

## Stream metrics to a central collector (the "Brain")

```gleam
config.new()
|> config.with_brain_stream(node: "brain@10.0.0.1", name: "ingest")
|> config.with_metric_spillover("/var/lib/pharos/spill.dets")
// optional: survive partitions
```

With a Brain configured, collected metrics are buffered in memory and shipped over distributed
Erlang (ETF) with backoff + jitter. Adding a spillover path enables a disk cold-tier that
preserves metrics during an outage and replays them (disk first) on reconnect.

## Shutting down

```gleam
pharos.stop(started.data)
```

Call `stop/1` from the process that owns the instance. It detaches the telemetry handler and
stops the supervision tree, and is safe to call more than once.

---

For the full picture, every `StatisticKind` and `Threshold`, custom probes, sink details, and
the resilient-pipeline internals, see the [usage guide](README.md) and
<https://hexdocs.pm/pharos>.
