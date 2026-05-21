package sourcemgr_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/beastmux/sourcemgr"
	"github.com/hyperized/demod1090/beast"
)

var (
	errTestDialFailedOnce = errors.New("test: dial failed once")
	errTestDialAlwaysFail = errors.New("test: dial always fails")
	errTestUnused         = errors.New("test: unused dial error")
)

// pipeDialer hands out pre-supplied net.Conn instances on each
// DialContext call. Used to drive Source against net.Pipe()
// connections without going through a real TCP listener.
type pipeDialer struct {
	mu     sync.Mutex
	queue  []func() (net.Conn, error)
	failed int
}

func (p *pipeDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.queue) == 0 {
		// Park until ctx cancels — keeps the Source's dial loop from
		// spinning out of test runs once the planned conns are
		// exhausted.
		<-ctx.Done()

		return nil, fmt.Errorf("pipeDialer: queue exhausted: %w", ctx.Err())
	}

	factory := p.queue[0]
	p.queue = p.queue[1:]

	conn, err := factory()
	if err != nil {
		p.failed++
	}

	return conn, err
}

// encodeFrame writes a single BEAST-encoded short frame to dst so
// the Source's beast.Reader has something well-formed to parse.
func encodeFrame(t *testing.T, payload []byte, ticks uint64, signal byte) []byte {
	t.Helper()

	out, err := beast.Encode(nil, payload, ticks, signal)
	if err != nil {
		t.Fatalf("beast.Encode: %v", err)
	}

	return out
}

// discardLogger silences slog output during tests so the streamed
// logs from Run don't pollute test output.
func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestSourceForwardsParsedFrames(t *testing.T) {
	t.Parallel()

	serverSide, clientSide := net.Pipe()

	dialer := &pipeDialer{
		queue: []func() (net.Conn, error){
			func() (net.Conn, error) { return clientSide, nil },
		},
	}

	payload := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}
	wire := encodeFrame(t, payload, 0xDEADBEEF, 0x42)

	go func() {
		defer func() { _ = serverSide.Close() }()

		_, _ = serverSide.Write(wire)

		// Hold the conn open so the reader doesn't EOF before the
		// frame propagates through bufio.
		time.Sleep(50 * time.Millisecond)
	}()

	source := sourcemgr.New("test-addr",
		sourcemgr.WithDialer(dialer),
		sourcemgr.WithBackoff(1*time.Millisecond, 10*time.Millisecond),
		sourcemgr.WithLogger(discardLogger()),
	)

	out := make(chan sourcemgr.Frame, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runDone := make(chan error, 1)

	go func() { runDone <- source.Run(ctx, out) }()

	select {
	case got := <-out:
		if got.Source != "test-addr" {
			t.Errorf("Frame.Source = %q, want %q", got.Source, "test-addr")
		}

		if string(got.Frame.Bytes) != string(payload) {
			t.Errorf("Frame.Bytes = %x, want %x", got.Frame.Bytes, payload)
		}

		if got.Frame.Ticks12MHz != 0xDEADBEEF {
			t.Errorf("Frame.Ticks12MHz = %x, want 0xDEADBEEF", got.Frame.Ticks12MHz)
		}

		if got.Frame.SignalLevel != 0x42 {
			t.Errorf("Frame.SignalLevel = %x, want 0x42", got.Frame.SignalLevel)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for frame from Source.Run")
	}

	if got := source.Frames(); got != 1 {
		t.Errorf("Frames() = %d, want 1", got)
	}

	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Source.Run did not exit within 2s of context cancellation")
	}
}

func TestSourceReconnectsAfterEOF(t *testing.T) {
	t.Parallel()

	firstServer, firstClient := net.Pipe()
	secondServer, secondClient := net.Pipe()

	dialer := &pipeDialer{
		queue: []func() (net.Conn, error){
			func() (net.Conn, error) { return firstClient, nil },
			func() (net.Conn, error) { return secondClient, nil },
		},
	}

	payload := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}
	wire := encodeFrame(t, payload, 1, 1)

	// First conn: deliver one frame, then EOF.
	go func() {
		_, _ = firstServer.Write(wire)

		time.Sleep(50 * time.Millisecond)

		_ = firstServer.Close()
	}()

	// Second conn: deliver one frame, hold open.
	go func() {
		defer func() { _ = secondServer.Close() }()

		_, _ = secondServer.Write(wire)

		time.Sleep(200 * time.Millisecond)
	}()

	source := sourcemgr.New("test-addr",
		sourcemgr.WithDialer(dialer),
		sourcemgr.WithBackoff(1*time.Millisecond, 5*time.Millisecond),
		sourcemgr.WithLogger(discardLogger()),
	)

	out := make(chan sourcemgr.Frame, 4)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runDone := make(chan struct{})

	go func() {
		_ = source.Run(ctx, out)

		close(runDone)
	}()

	// Expect two frames over two distinct connections.
	for index := range 2 {
		select {
		case <-out:
		case <-time.After(2 * time.Second):
			t.Fatalf("did not receive frame %d (Source did not reconnect after EOF)", index+1)
		}
	}

	if got := source.Frames(); got != 2 {
		t.Errorf("Frames() = %d, want 2 across two connections", got)
	}

	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Source.Run did not exit within 2s of context cancellation")
	}
}

func TestSourceRetriesAfterDialFailure(t *testing.T) {
	t.Parallel()

	serverSide, clientSide := net.Pipe()

	dialer := &pipeDialer{
		queue: []func() (net.Conn, error){
			func() (net.Conn, error) { return nil, errTestDialFailedOnce },
			func() (net.Conn, error) { return clientSide, nil },
		},
	}

	payload := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}
	wire := encodeFrame(t, payload, 1, 1)

	go func() {
		defer func() { _ = serverSide.Close() }()

		_, _ = serverSide.Write(wire)

		time.Sleep(100 * time.Millisecond)
	}()

	source := sourcemgr.New("test-addr",
		sourcemgr.WithDialer(dialer),
		sourcemgr.WithBackoff(1*time.Millisecond, 5*time.Millisecond),
		sourcemgr.WithLogger(discardLogger()),
	)

	out := make(chan sourcemgr.Frame, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runDone := make(chan struct{})

	go func() {
		_ = source.Run(ctx, out)

		close(runDone)
	}()

	select {
	case <-out:
	case <-time.After(2 * time.Second):
		t.Fatal("Source did not retry after the first dial failed")
	}

	cancel()

	<-runDone
}

func TestSourceDropsWhenChannelFull(t *testing.T) {
	t.Parallel()

	serverSide, clientSide := net.Pipe()

	dialer := &pipeDialer{
		queue: []func() (net.Conn, error){
			func() (net.Conn, error) { return clientSide, nil },
		},
	}

	payload := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}
	wire := encodeFrame(t, payload, 1, 1)

	// Stuff ten frames into the wire — only one can sit in the
	// 1-deep channel before the rest must drop.
	go func() {
		defer func() { _ = serverSide.Close() }()

		for range 10 {
			_, _ = serverSide.Write(wire)
		}

		time.Sleep(200 * time.Millisecond)
	}()

	source := sourcemgr.New("test-addr",
		sourcemgr.WithDialer(dialer),
		sourcemgr.WithBackoff(1*time.Millisecond, 5*time.Millisecond),
		sourcemgr.WithLogger(discardLogger()),
	)

	// Deliberately unbuffered would block; cap=1 with no consumer
	// fills after the first send and forces drops on the rest.
	out := make(chan sourcemgr.Frame, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runDone := make(chan struct{})

	go func() {
		_ = source.Run(ctx, out)

		close(runDone)
	}()

	// Give the source long enough to read, attempt send, and drop.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && source.Frames() < 10 {
		time.Sleep(10 * time.Millisecond)
	}

	if source.Frames() != 10 {
		t.Fatalf("Frames() = %d, want 10 (source did not drain the wire)", source.Frames())
	}

	if source.Drops() == 0 {
		t.Error("Drops() = 0, want > 0 (single-slot channel + 10 frames must produce drops)")
	}

	cancel()
	<-runDone
}

// alwaysFailDialer returns dialErr from every DialContext call so
// the Source spends the test inside the dial-error branch of Run.
type alwaysFailDialer struct{ dialErr error }

func (d *alwaysFailDialer) DialContext(_ context.Context, _, _ string) (net.Conn, error) {
	return nil, d.dialErr
}

func TestSourceExitsViaSleepCtxAfterDialFailure(t *testing.T) {
	t.Parallel()

	dialer := &alwaysFailDialer{dialErr: errTestDialAlwaysFail}

	source := sourcemgr.New("test-addr",
		sourcemgr.WithDialer(dialer),
		// Use a long backoff so we deterministically catch the
		// Source asleep in sleepCtx when cancel fires.
		sourcemgr.WithBackoff(time.Hour, time.Hour),
		sourcemgr.WithLogger(discardLogger()),
	)

	out := make(chan sourcemgr.Frame, 1)
	ctx, cancel := context.WithCancel(t.Context())

	runDone := make(chan error, 1)

	go func() { runDone <- source.Run(ctx, out) }()

	// Let Run fail the first dial and enter sleepCtx.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled (via sleepCtx after dial fail)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Source.Run did not exit via sleepCtx within 2s of cancel")
	}
}

// TestSourceBacksOffAfterNoFrameDisconnect covers the post-runOnce
// path where the connection closed without yielding any frame:
// the backoff must escalate before the next dial attempt. A short
// backoff lets multiple loop iterations run inside the 50 ms window
// so the post-sleep "no frames → bump backoff" line is exercised.
func TestSourceBacksOffAfterNoFrameDisconnect(t *testing.T) {
	t.Parallel()

	closedConn := func() (net.Conn, error) {
		_, client := net.Pipe()
		// Close immediately — reader.Frame will see io.EOF on the
		// first read with no frame having been delivered.
		_ = client.Close()

		return client, nil
	}

	dialer := &pipeDialer{
		queue: []func() (net.Conn, error){closedConn, closedConn, closedConn},
	}

	source := sourcemgr.New("test-addr",
		sourcemgr.WithDialer(dialer),
		sourcemgr.WithBackoff(1*time.Millisecond, 10*time.Millisecond),
		sourcemgr.WithLogger(discardLogger()),
	)

	out := make(chan sourcemgr.Frame, 1)
	ctx, cancel := context.WithCancel(t.Context())

	runDone := make(chan error, 1)

	go func() { runDone <- source.Run(ctx, out) }()

	// Let several iterations of dial → immediate-EOF → sleep →
	// backoff-bump run, then cancel. The pipeDialer parks on ctx
	// after exhausting its queue.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Source.Run did not exit within 2s of cancel after no-frame disconnect")
	}

	if got := source.Frames(); got != 0 {
		t.Errorf("Frames() = %d, want 0 (conn closed before any frame)", got)
	}
}

// TestSourceExitsViaSleepCtxAfterDisconnect drives the post-runOnce
// sleepCtx error path: a frame arrives (anyFrames=true), the conn
// closes, Run sleeps before reconnecting, and the cancellation hits
// during that sleep.
func TestSourceExitsViaSleepCtxAfterDisconnect(t *testing.T) {
	t.Parallel()

	serverSide, clientSide := net.Pipe()

	dialer := &pipeDialer{
		queue: []func() (net.Conn, error){
			func() (net.Conn, error) { return clientSide, nil },
		},
	}

	payload := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}
	wire := encodeFrame(t, payload, 1, 1)

	go func() {
		_, _ = serverSide.Write(wire)
		// Close so runOnce returns; Run then enters its post-
		// disconnect sleepCtx with backoff = backoffMin.
		_ = serverSide.Close()
	}()

	source := sourcemgr.New("test-addr",
		// Long backoff so cancel deterministically lands mid-sleep.
		sourcemgr.WithDialer(dialer),
		sourcemgr.WithBackoff(time.Hour, time.Hour),
		sourcemgr.WithLogger(discardLogger()),
	)

	out := make(chan sourcemgr.Frame, 1)
	ctx, cancel := context.WithCancel(t.Context())

	runDone := make(chan error, 1)

	go func() { runDone <- source.Run(ctx, out) }()

	// Wait for the frame to be received so runOnce has at least
	// processed one send.
	select {
	case <-out:
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive frame before disconnect")
	}

	// Give Run time to see the EOF, exit runOnce, pass the
	// `ctx.Err()` check, and enter sleepCtx. Without this the
	// cancel below races ahead and Run exits via the earlier
	// check (line 161) instead of the sleep wake-up.
	time.Sleep(100 * time.Millisecond)

	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Source.Run did not exit within 2s of cancel during post-disconnect sleep")
	}
}

// TestSourceReturnsImmediatelyWhenCtxAlreadyCancelled exercises the
// bottom `return ctx.Err()` in Run — the for-loop predicate fails on
// entry, so the body never runs.
func TestSourceReturnsImmediatelyWhenCtxAlreadyCancelled(t *testing.T) {
	t.Parallel()

	source := sourcemgr.New("test-addr",
		sourcemgr.WithDialer(&alwaysFailDialer{dialErr: errTestUnused}),
		sourcemgr.WithLogger(discardLogger()),
	)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := source.Run(ctx, make(chan sourcemgr.Frame))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run with pre-cancelled ctx = %v, want context.Canceled", err)
	}
}

// TestSourceExitsOnContextCancelMidRead asserts the watchdog
// goroutine inside runOnce closes the conn so a blocked
// reader.Frame() returns instead of leaking the goroutine.
func TestSourceExitsOnContextCancelMidRead(t *testing.T) {
	t.Parallel()

	serverSide, clientSide := net.Pipe()

	dialer := &pipeDialer{
		queue: []func() (net.Conn, error){
			func() (net.Conn, error) { return clientSide, nil },
		},
	}

	// Server side stays open and silent — the Source's reader.Frame
	// will block waiting for bytes. Closing the conn via ctx cancel
	// is the only path out.
	defer func() { _ = serverSide.Close() }()

	source := sourcemgr.New("test-addr",
		sourcemgr.WithDialer(dialer),
		sourcemgr.WithBackoff(1*time.Millisecond, 5*time.Millisecond),
		sourcemgr.WithLogger(discardLogger()),
	)

	out := make(chan sourcemgr.Frame, 1)
	ctx, cancel := context.WithCancel(t.Context())

	runDone := make(chan error, 1)

	go func() { runDone <- source.Run(ctx, out) }()

	// Let the source dial + park inside reader.Frame.
	time.Sleep(50 * time.Millisecond)

	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Source.Run did not exit within 2s of cancel; watchdog likely failed to close conn")
	}
}
