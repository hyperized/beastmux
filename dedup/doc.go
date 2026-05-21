// Package dedup is a short-window, size-bounded deduplicator for
// Mode S frames flowing through beastmux.
//
// # What it does
//
// Each BEAST frame received from any source is reduced to a 64-bit
// key over its raw Mode S payload (see Key). Seen records the key
// with a TTL of Window and reports whether the same key was already
// in-flight, so the caller can drop duplicates that arrive within
// the window from other sources.
//
// The key intentionally covers only the Mode S payload — not the
// BEAST envelope's 12 MHz timestamp or RSSI byte — because the same
// on-air frame received by two antennas produces byte-identical
// payloads while the envelope fields differ per receiver.
//
// # Why a TTL set
//
// Two receivers covering overlapping airspace see the same on-air
// frame within microseconds, but the BEAST publish path adds
// 50-200 ms of jitter and network propagation adds a few more.
// A window in the hundreds-of-milliseconds range catches the
// duplicate while keeping a genuine retransmission of the same
// payload seconds later visible as a fresh frame.
//
// First-arrival wins: the first Seen call for a key returns false
// (forward this frame) and subsequent calls inside the window
// return true (drop). This minimises end-to-end latency and avoids
// needing a per-frame deadline to wait for all sources.
//
// # Memory bound
//
// Seen opportunistically sweeps expired entries when the map
// crosses MaxSize. Under steady-state load (≤ 10 K frames/sec, 500
// ms window ≈ 5 K live entries) the default cap of 65536 leaves
// 6.5× headroom. Pathological inputs that fill the map with
// in-window entries beyond the cap let it grow past MaxSize — the
// cap is a sweep trigger, not a hard limit, because dropping the
// new frame would also drop dedup correctness.
//
// # Concurrency
//
// Seen is safe for concurrent use. The intended deployment is a
// single dedup goroutine fed by N source goroutines through a
// channel, so contention is not expected in practice — but the
// mutex is there for callers who want to drive Seen from multiple
// goroutines directly.
package dedup
