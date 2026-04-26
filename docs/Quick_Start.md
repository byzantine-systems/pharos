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

---

For the full picture, every `StatisticKind` and `Threshold`, custom probes, sink details, and
the resilient-pipeline internals, see the [usage guide](README.md) and
<https://hexdocs.pm/pharos>.
