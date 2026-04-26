# Pharos

[![Package Version](https://img.shields.io/hexpm/v/pharos)](https://hex.pm/packages/pharos)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/pharos/)
[![Erlang Compatible](https://img.shields.io/badge/target-erlang-b83998)](https://www.erlang.org/)
![License](https://img.shields.io/github/license/byzantine-systems/pharos)

[![Built with Nix](https://builtwithnix.org/badge.svg)](https://builtwithnix.org)
[![[Nix] Build & Test](https://github.com/byzantine-systems/pharos/actions/workflows/build.yml/badge.svg)](https://github.com/byzantine-systems/pharos/actions/workflows/build.yml)

> **BEACON / PHAROS (φάρος)**, (...) The system was reportedly created by [Leo the Mathematician](https://en.wikipedia.org/wiki/Leo_the_Mathematician), who devised a code tor the interpretation of signals, and had two identical water clocks (see Horologion) made for the terminal stations. His work took account of the difference in longitude and of the time the signal needed for transmission. Modern experiments suggest that one hour would suffice for the entire distance. The beacons consisted of huge bonfires on platforms or towers within fortifications on isolated hills. (...) [^1]
>
> [^1]: The Oxford Dictionary of Byzantium, Vol I.

**Pharos** is a decentralized "smart sensor" for the BEAM: a producer-only monitor and
alert manager that watches a node and its host, evaluates thresholds locally, and streams
metrics to a central collector, while staying alive through network partitions.

It is built as a plugin over [`:telemetry`](https://hex.pm/packages/telemetry) and
[`telemetry_poller`](https://hex.pm/packages/telemetry_poller), and runs as a supervised
OTP application that you embed in your own program via `pharos.start_link/1`.

## Features

- **Three-tier collection** out of the box:
  - **Host (OS):** memory (`/proc/meminfo`), disk and CPU (OTP `os_mon`), network throughput (`/proc/net/dev`).
  - **BEAM runtime:** memory, run queues, system counts, persistent terms, scheduler utilization, reductions.
  - **Application:** any `:telemetry` signal, via a pluggable custom-**probe** lane.
- **Local alerting engine**:
    - Per-threshold soak/cool state machines (built on [`eparch`](https://hex.pm/packages/eparch)) that debounce, de-duplicate, and auto-resolve alerts. Nothing fires on a transient blip; nothing stays stuck firing after recovery.
- **Resilient metric pipeline**:
    - A connection-manager `gen_statem` with exponential backoff and a jitter, a bounded in-memory hot buffer (ETS), and an optional disk-backed spillover (Dets) so metrics survive a partition and replay disk-first on reconnect.
- **Pluggable alert sinks**:
    - Console
    - Webhook (JSON)
    - Native BEAM-to-BEAM (ETF) with an OTLP exporter stub.
- **Type-safe configuration**:
    - A builder `Config` where every threshold maps 1:1 to a measured field, so adding a metric is a compiler-guided change.

## Installation

```sh
gleam add pharos
```

Pharos targets the Erlang runtime (OTP 27+). Host disk/CPU collection uses OTP's `os_mon`, native delivery uses distributed Erlang (ETF over the distribution protocol).

## Quick start

```gleam
import pharos
import pharos/alert
import pharos/config
import pharos/statistic

pub fn main() {
  let assert Ok(started) =
    pharos.start_link(
      config.new()
      |> config.with_statistics([
        statistic.poll(statistic.BeamMemory),
        statistic.poll_every(statistic.BeamRunQueues, 500),
        statistic.poll(statistic.HostCpu),
      ])
      |> config.with_thresholds([
        config.TotalMemory(above: 500.0),
        config.HostCpuUtil(above: 85.0),
      ]),
    )

  // React to firing / resolved alerts on the event bus.
  let assert Ok(_handler) =
    pharos.subscribe(started.data, fn(event) {
      case event {
        alert.AlertFiring(id:, level:, diagnostic:) -> handle_firing(id, level, diagnostic)
        alert.AlertResolved(id:) -> handle_resolved(id)
      }
    })

  // ... later, when shutting the owning process down:
  // pharos.stop(started.data)
}
```

See the [Quick Start](docs/Quick_Start.md) and the [usage guide & architecture](docs/README.md)
for thresholds, custom probes, alert sinks, and the resilient Brain metric stream.

## Documentation

- [Usage guide & architecture](docs/README.md)
- [Quick Start](docs/Quick_Start.md)
- API reference: <https://hexdocs.pm/pharos>

## Development

```sh
# run the test suite
gleam test
# format
gleam format        
# build API docs
gleam docs build    
```

This repository is also a Nix flake: `nix build`, `nix develop`, `nix flake check`.

> Pharos is one of three sibling projects under `byzantine-systems`: it builds on **eparch**
(type-safe OTP behaviour wrappers) and is the intended monitoring layer for **logothetes**
(NixOS MicroVM orchestration), which it can watch over the custom-probe lane without taking
a dependency on it.

## License

LGPL-3.0-or-later. See [LICENSE](LICENSE).
