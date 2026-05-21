package sourcemgr

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/hyperized/demod1090/beast"
)

// DefaultBackoffMin is the initial reconnect delay after a failed
// dial or a connection that produced no frames.
const DefaultBackoffMin = 1 * time.Second

// DefaultBackoffMax is the cap on the reconnect delay. Doubling
// stops here.
const DefaultBackoffMax = 30 * time.Second

// DefaultDialTimeout is the per-attempt TCP dial timeout used by
// the default net.Dialer. Long-lived sources reconnect on
// backoff; the dial timeout only limits a single attempt.
const DefaultDialTimeout = 10 * time.Second

// Frame is the unit published downstream — the parsed BEAST frame
// plus the source address that produced it so a consumer can
// attribute counters.
type Frame struct {
	Source string
	Frame  beast.Frame
}

// Dialer is the minimal subset of *net.Dialer the Source needs.
// Tests substitute an in-memory connection factory.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// Source streams BEAST frames from one upstream TCP endpoint. The
// zero value is unusable; construct via New.
type Source struct {
	addr       string
	backoffMin time.Duration
	backoffMax time.Duration
	dialer     Dialer
	logger     *slog.Logger

	frames atomic.Uint64
	drops  atomic.Uint64
}

// Option configures a Source at construction.
type Option func(*Source)

// WithBackoff overrides the reconnect backoff bounds. min must be
// > 0 and max must be ≥ min; invalid pairs are ignored and the
// defaults stay in place.
func WithBackoff(minBackoff, maxBackoff time.Duration) Option {
	return func(source *Source) {
		if minBackoff <= 0 || maxBackoff < minBackoff {
			return
		}

		source.backoffMin = minBackoff
		source.backoffMax = maxBackoff
	}
}

// WithDialer overrides the default *net.Dialer. A nil dialer is
// ignored.
func WithDialer(dialer Dialer) Option {
	return func(source *Source) {
		if dialer != nil {
			source.dialer = dialer
		}
	}
}

// WithLogger overrides slog.Default(). A nil logger is ignored.
func WithLogger(logger *slog.Logger) Option {
	return func(source *Source) {
		if logger != nil {
			source.logger = logger
		}
	}
}

// New returns a Source pointed at addr with the supplied options.
func New(addr string, opts ...Option) *Source {
	source := &Source{
		addr:       addr,
		backoffMin: DefaultBackoffMin,
		backoffMax: DefaultBackoffMax,
		dialer:     &net.Dialer{Timeout: DefaultDialTimeout},
		logger:     slog.Default(),
	}

	for _, opt := range opts {
		opt(source)
	}

	return source
}

// Addr returns the upstream address the Source dials.
func (s *Source) Addr() string {
	return s.addr
}

// Frames returns the cumulative number of frames parsed from the
// upstream (regardless of whether they were forwarded or dropped).
func (s *Source) Frames() uint64 {
	return s.frames.Load()
}

// Drops returns the cumulative number of frames that could not be
// sent because the output channel was full.
func (s *Source) Drops() uint64 {
	return s.drops.Load()
}

// Run blocks until ctx is cancelled, dialing the upstream and
// publishing parsed frames on out. Non-blocking sends drop frames
// when out is full; see Drops.
//
// Returns ctx.Err() once ctx is cancelled. Returns any error from
// sleepCtx (the only error path that can surface non-context
// errors today is the dial loop, where the dial error is logged
// and retried, not returned).
func (s *Source) Run(ctx context.Context, out chan<- Frame) error {
	backoff := s.backoffMin

	for ctx.Err() == nil {
		conn, err := s.dialer.DialContext(ctx, "tcp", s.addr)
		if err != nil {
			s.logger.LogAttrs(ctx, slog.LevelWarn, "beastmux: dial failed",
				slog.String("addr", s.addr),
				slog.Duration("retry_after", backoff),
				slog.Any("err", err))

			if waitErr := sleepCtx(ctx, backoff); waitErr != nil {
				return waitErr
			}

			backoff = nextBackoff(backoff, s.backoffMax)

			continue
		}

		anyFrames, runErr := s.runOnce(ctx, conn, out)
		if anyFrames {
			backoff = s.backoffMin
		}

		s.logger.LogAttrs(ctx, slog.LevelInfo, "beastmux: source disconnected",
			slog.String("addr", s.addr),
			slog.Bool("any_frames", anyFrames),
			slog.Any("err", runErr))

		if ctx.Err() != nil {
			return fmt.Errorf("sourcemgr: %w", ctx.Err())
		}

		if waitErr := sleepCtx(ctx, backoff); waitErr != nil {
			return waitErr
		}

		if !anyFrames {
			backoff = nextBackoff(backoff, s.backoffMax)
		}
	}

	return fmt.Errorf("sourcemgr: %w", ctx.Err())
}

// runOnce drives a single connected reader. Returns whether at
// least one frame was successfully parsed (used by Run to reset
// backoff) and the terminal error from beast.Reader.
func (s *Source) runOnce(ctx context.Context, conn net.Conn, out chan<- Frame) (bool, error) {
	defer func() { _ = conn.Close() }()

	// Close the conn when ctx cancels so the in-flight reader.Frame
	// call returns instead of blocking forever. The watcher exits
	// on close(stop), which fires when runOnce returns normally.
	stop := make(chan struct{})
	defer close(stop)

	go watchAndClose(ctx, conn, stop)

	reader := beast.NewReader(conn)

	var anyFrames bool

	for {
		frame, err := reader.Frame()
		if err != nil {
			return anyFrames, fmt.Errorf("sourcemgr: read frame: %w", err)
		}

		anyFrames = true

		s.frames.Add(1)

		envelope := Frame{Source: s.addr, Frame: frame}

		select {
		case out <- envelope:
		default:
			// Channel full — drop. ctx cancellation is observed on
			// the next reader.Frame() call (the watchdog closes the
			// conn so that read returns immediately).
			s.drops.Add(1)
		}
	}
}

// watchAndClose closes conn when ctx is cancelled, so a blocked
// reader.Frame() returns immediately. Exits without touching conn
// when stop is closed (the normal runOnce-returning path).
func watchAndClose(ctx context.Context, conn net.Conn, stop <-chan struct{}) {
	select {
	case <-ctx.Done():
		_ = conn.Close()
	case <-stop:
	}
}
