# beastmux

Multiplex N upstream Mode S BEAST TCP streams into one consolidated, deduplicated stream.

`beastmux` is the dual of [`beast-splitter`](https://github.com/openskynetwork/beast-splitter): splitter fans one BEAST source out to many consumers; beastmux fans many BEAST sources into one consumer. It does not demodulate — it operates entirely at the TCP tier on already-decoded BEAST frames.

## Use case

Multiple ADS-B receivers covering overlapping airspace (different antennas, different sites on the same network) each publish their decoded frames on `:30005` (the `dump1090` / `readsb` / `demod1090` convention). A consumer downstream — a feeder, a tar1090 instance, a logger — wants the combined recall of all receivers without paying for N× the frame volume from duplicates.

```
  RTL-SDR A → demod1090 (BEAST :30005) ─┐
  RTL-SDR B → demod1090 (BEAST :30005) ─┼─→ beastmux ─→ BEAST :30005 → tar1090 / feeder / logger
  RTL-SDR C → demod1090 (BEAST :30005) ─┘
```

## Scope

- Pure Go, stdlib networking, no CGo. Single static binary.
- TCP-in / TCP-out only. No demodulation, no decoding above frame-bytes level.
- Payload-only dedup over a short sliding window (default 500 ms).
- First-arrival wins — minimises end-to-end latency, avoids per-frame deadlines.

Out of scope: any radio I/O, frame decoding (DF parsing, CPR, callsigns, etc. — that's [`hyperized/modes`](https://github.com/hyperized/modes)), TLS termination (use `stunnel`, WireGuard, or `socat`), and multi-process clustering.

## Layout

```
dedup/          TTL'd 64-bit-key set + hash/maphash keying helper
sourcemgr/      Per-upstream TCP dialer + beast.Reader + reconnect/backoff
cmd/beastmux/   CLI entry point — wires sources → dedup → beastsrv
contrib/systemd Sample beastmux.service unit
```

`cmd/beastmux` depends on [`hyperized/demod1090`](https://github.com/hyperized/demod1090) for the streaming `beast.Reader` and the `beastsrv` fanout server.

## Build & test

```sh
make                 # fmt + vet + test (race + cover) — the default
make lint            # golangci-lint run ./...
make cover           # write coverage.html alongside coverage.out
make build           # build cmd/beastmux for the host
make build-aarch64   # cross-compile cmd/beastmux for linux/arm64
```

Integration tests live behind the `integration` build tag and stay out of the default `make test`:

```sh
make test-integration
```

## Usage

```sh
beastmux \
    --source receiver-a:30005 \
    --source receiver-b:30005 \
    --source receiver-c:30005 \
    --listen :30005
```

That binds one consolidated BEAST listener on `:30005` and pulls from three upstreams. `--source` is repeatable; at least one is required.

### Flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `--source host:port` | — (required) | Upstream BEAST endpoint. Repeatable. |
| `--listen host:port` | `:30005` | Consolidated BEAST listener. Empty disables the server (useful with `--stdout-hex`). |
| `--dedup-window` | `500ms` | TTL applied to each payload key. |
| `--reconnect-min` | `1s` | Initial backoff after a source disconnects / fails to dial. |
| `--reconnect-max` | `30s` | Cap on reconnect backoff. |
| `--stats-interval` | `10s` | Stats log cadence. `0` disables. |
| `--stdout-hex` | `false` | Also emit each forwarded frame as a lowercase hex line on stdout. |
| `--log-format` | `text` | `text` or `json`. |
| `--version` | — | Print version and exit. |

### Dedup keying

The key is `hash/maphash` over the raw Mode S payload only — the BEAST envelope's 12 MHz timestamp and RSSI byte are deliberately excluded so two receivers seeing the same on-air frame produce the same key. The seed is randomised per process start; keys are not stable across runs.

A frame is a duplicate iff its key was seen within `--dedup-window`. The default of 500 ms covers ~50–200 ms of BEAST publish jitter plus a few ms of LAN propagation, while being short enough that a genuine retransmission seconds later counts as a fresh frame.

### Backpressure

The shared source-to-dedup channel is buffered at 1024 frames. Each source uses a non-blocking send: if the channel is full, the frame is counted as a drop on that source rather than back-pressuring the upstream TCP read. The stats log line surfaces per-source drop counts; sustained drops indicate the downstream consumer can't keep up.

## Deployment recipes

### Single host: collocated with multiple dump1090 / demod1090 instances

The simplest case — one Pi runs several receivers (different antennas, different gain settings) plus beastmux to consolidate.

```sh
# Demod processes on local ports 30005, 30006, 30007 (one per dongle).
beastmux \
    --source 127.0.0.1:30005 \
    --source 127.0.0.1:30006 \
    --source 127.0.0.1:30007 \
    --listen :30100
```

Point tar1090 / readsb at `:30100` as their BEAST input. Leave the original receiver ports for direct debugging.

### Multi-host via SSH reverse tunnel

Two sites, no public IPs, one wants the other's frames. BEAST has no authentication or encryption — never expose it on a public network. SSH carries the bytes.

On the producer host:

```sh
# Reverse-tunnel the producer's :30005 onto the consumer's loopback :30006
ssh -N -R 30006:127.0.0.1:30005 consumer-host
```

On the consumer host:

```sh
beastmux \
    --source 127.0.0.1:30005 \
    --source 127.0.0.1:30006 \
    --listen :30100
```

For permanent links use `autossh` or a systemd `ssh.service` with `ServerAliveInterval=15`/`ServerAliveCountMax=3`.

### Sidecar to tar1090

tar1090 expects a BEAST stream on `:30005`. If the box has both a local receiver and remote ones, run beastmux on `:30005` and move the local receiver to a different port.

```sh
# Local demod1090 publishes on :30006 (was :30005).
# Remote site is reachable via SSH tunnel on :30007.
beastmux \
    --source 127.0.0.1:30006 \
    --source 127.0.0.1:30007 \
    --listen :30005 \
    --stats-interval 30s \
    --log-format json
```

tar1090 dials `:30005` as before. Stats land in journald as one structured log line every 30 s.

## Operations

### Stats log line

Default `--stats-interval 10s` emits one `slog.Info` record per interval with per-source frame / drop deltas, dedup ratio, and forwarded count. JSON mode (`--log-format json`) makes it grep-friendly:

```json
{"time":"...","level":"INFO","msg":"beastmux: stats","interval":"10s",
 "seen":4123,"dupes":1872,"forwarded":2251,"dedup_ratio":0.453,
 "receiver-a:30005":{"frames":2110,"drops":0},
 "receiver-b:30005":{"frames":2013,"drops":0}}
```

`dedup_ratio` rising above ~0.5 with two well-aligned receivers is normal. Approaching 0 means your sources don't overlap; the dedup buys nothing and the second source is purely additive coverage.

### systemd

`contrib/systemd/beastmux.service` ships a hardened sample unit. Install:

```sh
sudo install -m 0644 contrib/systemd/beastmux.service /etc/systemd/system/
sudo useradd --system --no-create-home --shell /usr/sbin/nologin beastmux
sudo install -m 0755 -o beastmux -g beastmux beastmux /opt/beastmux/beastmux
sudo systemctl daemon-reload
sudo systemctl enable --now beastmux.service
```

Then edit the `ExecStart` line in the unit to point at your real upstream addresses. The unit defaults to `--log-format json` so the stats line flows through journald cleanly. Stdout is dropped to keep `--stdout-hex` (if you enable it for debugging) from flooding the journal.

### Graceful shutdown

`SIGINT` and `SIGTERM` cancel the daemon's root context. Sources exit, in-flight publishes drain, the listener closes, and the process exits with status 0. `systemctl stop` and Ctrl-C both go through this path.

### Hot-reload

There is no reload — beastmux is restart-on-change. The systemd unit's `Restart=on-failure` only covers crashes; configuration changes need `systemctl restart beastmux`.

### TLS / authentication

BEAST has neither. For untrusted networks, terminate TLS externally:

```sh
# stunnel example on the producer host: accept TLS on :30443, forward to :30005.
stunnel -fd 0 <<'EOF'
[beast]
accept = 30443
connect = 127.0.0.1:30005
cert = /etc/stunnel/server.pem
EOF
```

Or use WireGuard / Tailscale for transport-layer encryption + auth and let beastmux dial the WireGuard-local address.

## License

[Business Source License 1.1](LICENSE). Free for non-production use; production use requires a separate licence. Change Date: see `LICENSE`.
