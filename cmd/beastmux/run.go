package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperized/beastmux/dedup"
	"github.com/hyperized/beastmux/sourcemgr"
	"github.com/hyperized/demod1090/beastsrv"
)

// framesChanCap is the buffer depth on the shared sources → dedup
// channel. 1024 frames is roughly 100 ms of headroom at 10 K
// frames/sec; sources non-blocking-send and count drops past that.
const framesChanCap = 1024

// errNoSources signals that run was invoked without any --source
// entries. main exits with status 2 on this path.
var errNoSources = errors.New("beastmux: at least one --source is required")

// config carries everything run needs to wire the daemon. main
// populates server and hexOut from flags after parseFlags returns;
// integration tests inject their own to grab the bound address or
// capture hex output. A nil hexOut disables --stdout-hex regardless
// of the stdoutHex flag.
type config struct {
	sources       []string
	listenAddr    string
	dedupWindow   time.Duration
	reconnectMin  time.Duration
	reconnectMax  time.Duration
	statsInterval time.Duration
	stdoutHex     bool
	showVersion   bool
	logFormat     string
	server        *beastsrv.Server
	hexOut        io.Writer
}

// dedupStats tracks the cumulative seen + duplicate counts the
// dedup goroutine observes. Reporter goroutine snapshots these
// every stats-interval to emit deltas.
type dedupStats struct {
	seen  atomic.Uint64
	dupes atomic.Uint64
}

// run wires the source managers, the single dedup goroutine, and
// (optionally) the BEAST server. Blocks until ctx is cancelled.
func run(ctx context.Context, cfg config, logger *slog.Logger) error {
	if len(cfg.sources) == 0 {
		return errNoSources
	}

	deduper := dedup.New(dedup.WithWindow(cfg.dedupWindow))
	framesCh := make(chan sourcemgr.Frame, framesChanCap)
	stats := &dedupStats{}

	sources := make([]*sourcemgr.Source, 0, len(cfg.sources))

	var waitGroup sync.WaitGroup

	for _, addr := range cfg.sources {
		source := sourcemgr.New(addr,
			sourcemgr.WithBackoff(cfg.reconnectMin, cfg.reconnectMax),
			sourcemgr.WithLogger(logger),
		)
		sources = append(sources, source)

		waitGroup.Go(func() { _ = source.Run(ctx, framesCh) })
	}

	if cfg.server != nil {
		waitGroup.Go(func() {
			if err := cfg.server.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.LogAttrs(ctx, slog.LevelError, "beastmux: server stopped",
					slog.Any("err", err))
			}
		})
	}

	waitGroup.Go(func() {
		dedupLoop(ctx, framesCh, deduper, cfg.server, cfg.hexOut, stats)
	})

	if cfg.statsInterval > 0 {
		waitGroup.Go(func() {
			reportStats(ctx, cfg.statsInterval, sources, stats, logger)
		})
	}

	waitGroup.Wait()

	return nil
}

// dedupLoop drains framesCh, drops duplicates inside the configured
// window, and forwards the rest to the BEAST server and/or hexOut.
// Single goroutine by design — keeps the dedup map effectively lock-
// free (only this writer touches it) and serialises publishes so
// downstream sees one consolidated stream.
//
// A nil server skips the BEAST fanout; a nil hexOut skips the hex
// emit. Both nil reduces dedupLoop to a counting drain (useful in
// tests that exercise the channel plumbing).
func dedupLoop(
	ctx context.Context,
	framesCh <-chan sourcemgr.Frame,
	deduper *dedup.Dedup,
	server *beastsrv.Server,
	hexOut io.Writer,
	stats *dedupStats,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case envelope, ok := <-framesCh:
			if !ok {
				return
			}

			stats.seen.Add(1)

			key := dedup.Key(envelope.Frame.Bytes)
			if deduper.Seen(key, time.Now()) {
				stats.dupes.Add(1)

				continue
			}

			if server != nil {
				server.Publish(envelope.Frame)
			}

			if hexOut != nil {
				_, _ = fmt.Fprintln(hexOut, hex.EncodeToString(envelope.Frame.Bytes))
			}
		}
	}
}

// reportStats emits one slog.Info line per interval with the
// per-interval frame / drop deltas for each source plus the dedup
// ratio. Snapshots counters at each tick so the values are
// interval-deltas, not cumulatives. Returns on ctx cancellation.
func reportStats(
	ctx context.Context,
	interval time.Duration,
	sources []*sourcemgr.Source,
	stats *dedupStats,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prev := snapshotStats(sources, stats)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := snapshotStats(sources, stats)
			emitStats(ctx, logger, current.sub(prev), interval)
			prev = current
		}
	}
}

// statsSnapshot is a point-in-time read of all counters the
// reporter cares about. The reporter takes one at each tick and
// subtracts the previous snapshot to log per-interval deltas.
type statsSnapshot struct {
	sources []sourceSnapshot
	seen    uint64
	dupes   uint64
}

type sourceSnapshot struct {
	addr   string
	frames uint64
	drops  uint64
}

func snapshotStats(sources []*sourcemgr.Source, stats *dedupStats) statsSnapshot {
	snap := statsSnapshot{
		sources: make([]sourceSnapshot, len(sources)),
		seen:    stats.seen.Load(),
		dupes:   stats.dupes.Load(),
	}

	for index, source := range sources {
		snap.sources[index] = sourceSnapshot{
			addr:   source.Addr(),
			frames: source.Frames(),
			drops:  source.Drops(),
		}
	}

	return snap
}

// sub returns the per-source and dedup deltas between s and prior.
// Counters are monotonic, so prior is always ≤ s; underflow would
// indicate a counter reset or a bug.
func (s statsSnapshot) sub(prior statsSnapshot) statsSnapshot {
	delta := statsSnapshot{
		sources: make([]sourceSnapshot, len(s.sources)),
		seen:    s.seen - prior.seen,
		dupes:   s.dupes - prior.dupes,
	}

	for index, source := range s.sources {
		base := sourceSnapshot{addr: source.addr}
		if index < len(prior.sources) {
			base = prior.sources[index]
		}

		delta.sources[index] = sourceSnapshot{
			addr:   source.addr,
			frames: source.frames - base.frames,
			drops:  source.drops - base.drops,
		}
	}

	return delta
}

func emitStats(ctx context.Context, logger *slog.Logger, delta statsSnapshot, interval time.Duration) {
	forwarded := delta.seen - delta.dupes

	dedupRatio := 0.0
	if delta.seen > 0 {
		dedupRatio = float64(delta.dupes) / float64(delta.seen)
	}

	const baseAttrs = 5

	attrs := make([]slog.Attr, 0, baseAttrs+len(delta.sources))
	attrs = append(attrs,
		slog.Duration("interval", interval),
		slog.Uint64("seen", delta.seen),
		slog.Uint64("dupes", delta.dupes),
		slog.Uint64("forwarded", forwarded),
		slog.Float64("dedup_ratio", dedupRatio),
	)

	for _, source := range delta.sources {
		attrs = append(attrs, slog.Group(source.addr,
			slog.Uint64("frames", source.frames),
			slog.Uint64("drops", source.drops),
		))
	}

	logger.LogAttrs(ctx, slog.LevelInfo, "beastmux: stats", attrs...)
}
