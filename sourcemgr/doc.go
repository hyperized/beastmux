// Package sourcemgr manages one upstream BEAST source: dial,
// parse, forward, reconnect.
//
// # Lifecycle
//
// A Source is created with New(addr, opts...) and driven by Run,
// which blocks until its context is cancelled. Run dials the
// upstream, parses incoming frames with demod1090's streaming
// beast.Reader, and publishes each parsed frame on the channel the
// caller supplies. On read error or EOF, Run reconnects with
// exponential backoff bounded by [BackoffMin, BackoffMax].
//
// The backoff resets to BackoffMin whenever a connection produced
// at least one frame, so a steady source that occasionally drops
// reconnects fast; a misconfigured / unreachable source backs off
// to the cap and stays there until it starts producing.
//
// # Backpressure policy
//
// Sends to the output channel are non-blocking. If the channel is
// full when a frame arrives, the frame is dropped and the per-
// source drop counter increments. This deliberately decouples
// upstream TCP read pacing from downstream consumer pacing — a
// slow consumer can never wedge the read loop and force kernel
// recv-buffer overruns on the source side.
//
// # Concurrency
//
// Each Source has exactly one read goroutine driving the reader
// and writing to the output channel. Multiple Sources share one
// output channel; the consumer is responsible for serialising
// drains. Drops and Frames counters use atomics, so callers can
// poll them from any goroutine.
package sourcemgr
