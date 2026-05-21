package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/beastmux/dedup"
	"github.com/hyperized/beastmux/sourcemgr"
	"github.com/hyperized/demod1090/beast"
	"github.com/hyperized/demod1090/beastsrv"
)

const (
	flagSource = "--source"
	hostA      = "host-a:30005"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestParseFlagsAppliesDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := parseFlags([]string{flagSource, hostA}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags returned %v, want nil", err)
	}

	want := defaultConfig()
	want.sources = []string{hostA}

	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("parseFlags() = %+v, want %+v", cfg, want)
	}
}

func TestParseFlagsRepeatsSources(t *testing.T) {
	t.Parallel()

	cfg, err := parseFlags([]string{
		flagSource, hostA,
		flagSource, "host-b:30005",
		flagSource, "host-c:30005",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags returned %v, want nil", err)
	}

	want := []string{hostA, "host-b:30005", "host-c:30005"}
	if !reflect.DeepEqual(cfg.sources, want) {
		t.Errorf("cfg.sources = %v, want %v", cfg.sources, want)
	}
}

func TestParseFlagsHonoursOverrides(t *testing.T) {
	t.Parallel()

	cfg, err := parseFlags([]string{
		flagSource, hostA,
		"--listen", "127.0.0.1:31000",
		"--dedup-window", "250ms",
		"--reconnect-min", "2s",
		"--reconnect-max", "1m",
		"--stats-interval", "5s",
		"--stdout-hex",
		"--log-format", logFormatJSON,
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags returned %v, want nil", err)
	}

	if cfg.listenAddr != "127.0.0.1:31000" {
		t.Errorf("listenAddr = %q, want 127.0.0.1:31000", cfg.listenAddr)
	}

	if cfg.dedupWindow != 250*time.Millisecond {
		t.Errorf("dedupWindow = %v, want 250ms", cfg.dedupWindow)
	}

	if cfg.reconnectMin != 2*time.Second {
		t.Errorf("reconnectMin = %v, want 2s", cfg.reconnectMin)
	}

	if cfg.reconnectMax != time.Minute {
		t.Errorf("reconnectMax = %v, want 1m", cfg.reconnectMax)
	}

	if cfg.statsInterval != 5*time.Second {
		t.Errorf("statsInterval = %v, want 5s", cfg.statsInterval)
	}

	if !cfg.stdoutHex {
		t.Error("stdoutHex = false, want true")
	}

	if cfg.logFormat != logFormatJSON {
		t.Errorf("logFormat = %q, want %q", cfg.logFormat, logFormatJSON)
	}
}

func TestParseFlagsVersionShortCircuitsValidation(t *testing.T) {
	t.Parallel()

	// --version is accepted even without --source; main prints the
	// version and exits before run() is called.
	cfg, err := parseFlags([]string{"--version"}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags --version returned %v, want nil", err)
	}

	if !cfg.showVersion {
		t.Error("showVersion = false, want true")
	}
}

func TestParseFlagsRejectsNoSources(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer

	if _, err := parseFlags([]string{}, &stderr); err == nil {
		t.Fatal("parseFlags with no --source returned nil error")
	}

	if !strings.Contains(stderr.String(), flagSource) {
		t.Errorf("stderr did not mention --source: %q", stderr.String())
	}
}

func TestParseFlagsRejectsBadLogFormat(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer

	_, err := parseFlags([]string{flagSource, "host:30005", "--log-format", "xml"}, &stderr)
	if err == nil {
		t.Fatal("parseFlags with --log-format=xml returned nil error")
	}

	if !strings.Contains(stderr.String(), "log-format") {
		t.Errorf("stderr did not mention log-format: %q", stderr.String())
	}
}

func TestParseFlagsRejectsUnknownFlag(t *testing.T) {
	t.Parallel()

	_, err := parseFlags([]string{"--bogus"}, io.Discard)
	if err == nil {
		t.Fatal("parseFlags with --bogus returned nil error")
	}
}

func TestInvalidFlagErrorMessage(t *testing.T) {
	t.Parallel()

	err := InvalidFlagError("--source is required")
	if got, want := err.Error(), "beastmux: --source is required"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestStringSliceFlagRejectsEmpty(t *testing.T) {
	t.Parallel()

	var slice stringSliceFlag

	if err := slice.Set(""); err == nil {
		t.Error("Set(\"\") returned nil, want error")
	}

	if err := slice.Set("ok"); err != nil {
		t.Errorf("Set(\"ok\") returned %v, want nil", err)
	}

	if got := slice.String(); got != "ok" {
		t.Errorf("String() = %q, want %q", got, "ok")
	}
}

func TestNewLoggerHandlerType(t *testing.T) {
	t.Parallel()

	textLogger := newLogger("text", io.Discard)
	if _, ok := textLogger.Handler().(*slog.TextHandler); !ok {
		t.Errorf("newLogger(\"text\") handler = %T, want *slog.TextHandler", textLogger.Handler())
	}

	jsonLogger := newLogger(logFormatJSON, io.Discard)
	if _, ok := jsonLogger.Handler().(*slog.JSONHandler); !ok {
		t.Errorf("newLogger(\"json\") handler = %T, want *slog.JSONHandler", jsonLogger.Handler())
	}

	// Unknown format falls back to text (defensive — parseFlags
	// rejects the value before this is reached).
	fallback := newLogger("xml", io.Discard)
	if _, ok := fallback.Handler().(*slog.TextHandler); !ok {
		t.Errorf("newLogger(unknown) handler = %T, want *slog.TextHandler", fallback.Handler())
	}
}

func TestRunErrorsWithNoSources(t *testing.T) {
	t.Parallel()

	err := run(t.Context(), config{}, discardLogger())
	if !errors.Is(err, errNoSources) {
		t.Errorf("run with no sources = %v, want errNoSources", err)
	}
}

// TestDedupLoopForwardsAndDedupes feeds the loop directly: three
// envelopes (two duplicates, one unique) and asserts the server
// publishes exactly two distinct frames.
func TestDedupLoopForwardsAndDedupes(t *testing.T) {
	t.Parallel()

	server := beastsrv.NewServer(
		beastsrv.WithListenAddr("127.0.0.1:0"),
		beastsrv.WithLogger(discardLogger()),
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	serverDone := make(chan struct{})

	go func() {
		_ = server.Start(ctx)

		close(serverDone)
	}()

	waitForBind(t, server)

	conn, err := dialServer(t, server)
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}

	defer func() { _ = conn.Close() }()

	waitForClient(t, server, 1)

	deduper := dedup.New(dedup.WithWindow(500 * time.Millisecond))
	framesCh := make(chan sourcemgr.Frame, 4)

	payloadA := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}
	payloadB := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x19}

	framesCh <- sourcemgr.Frame{Source: "a", Frame: beast.Frame{Bytes: payloadA, Ticks12MHz: 1, SignalLevel: 1}}

	framesCh <- sourcemgr.Frame{Source: "b", Frame: beast.Frame{Bytes: payloadA, Ticks12MHz: 2, SignalLevel: 2}}

	framesCh <- sourcemgr.Frame{Source: "a", Frame: beast.Frame{Bytes: payloadB, Ticks12MHz: 3, SignalLevel: 3}}

	loopDone := make(chan struct{})

	go func() {
		dedupLoop(ctx, framesCh, deduper, server, nil, &dedupStats{})

		close(loopDone)
	}()

	// Read up to two frames from the wire; a third would mean a
	// duplicate slipped through.
	received := readFrames(t, conn, 2, 500*time.Millisecond)
	if len(received) != 2 {
		t.Fatalf("received %d frames, want 2", len(received))
	}

	// Order-independent comparison: the two payloads must each appear once.
	if !sameSet(received, [][]byte{payloadA, payloadB}) {
		t.Errorf("received payloads = %x, want %x and %x", received, payloadA, payloadB)
	}

	// One extra read with a tight deadline must time out (no dupe).
	if extras := readFrames(t, conn, 1, 100*time.Millisecond); len(extras) != 0 {
		t.Errorf("received %d extra frames, want 0 (duplicate suppression broken)", len(extras))
	}

	cancel()
	<-loopDone
	<-serverDone
}

// TestDedupLoopExitsOnClosedChannel exercises the `!ok` arm of the
// receive — a closed framesCh is the second exit path alongside ctx
// cancellation.
func TestDedupLoopExitsOnClosedChannel(t *testing.T) {
	t.Parallel()

	deduper := dedup.New()
	framesCh := make(chan sourcemgr.Frame)

	close(framesCh)

	done := make(chan struct{})

	go func() {
		dedupLoop(t.Context(), framesCh, deduper, nil, nil, &dedupStats{})

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dedupLoop did not exit on closed channel within 1s")
	}
}

// captureHandler is a slog.Handler that retains every record it
// receives so tests can assert on log output. Safe for concurrent
// Handle calls; tests should snapshot under Lock before asserting.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (*captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, record)

	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(_ string) slog.Handler { return h }

// TestRunLogsServerStartFailure covers the
// `cfg.server.Start(...) error → log` branch in run by giving
// beastsrv an already-bound listen address so its bind fails.
func TestRunLogsServerStartFailure(t *testing.T) {
	t.Parallel()

	listenCfg := &net.ListenConfig{}

	blocker, err := listenCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("blocker listen: %v", err)
	}

	defer func() { _ = blocker.Close() }()

	server := beastsrv.NewServer(
		beastsrv.WithListenAddr(blocker.Addr().String()),
		beastsrv.WithLogger(discardLogger()),
	)

	handler := &captureHandler{}

	cfg := config{
		// 127.0.0.1:0 is not a valid dial target — the source loops
		// failing dials forever, which is harmless during the test
		// window and keeps run() past the errNoSources check.
		sources:       []string{"127.0.0.1:0"},
		dedupWindow:   time.Second,
		reconnectMin:  10 * time.Millisecond,
		reconnectMax:  50 * time.Millisecond,
		statsInterval: 50 * time.Millisecond, // also exercises the reporter-spawn branch
		server:        server,
	}

	ctx, cancel := context.WithCancel(t.Context())

	runDone := make(chan error, 1)

	go func() { runDone <- run(ctx, cfg, slog.New(handler)) }()

	// Poll for the failure log up to a deadline.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if findRecord(handler, "server stopped") {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if !findRecord(handler, "server stopped") {
		t.Error("expected log record matching 'server stopped'; got none")
	}

	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("run returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not exit within 2s of cancel")
	}
}

func findRecord(h *captureHandler, substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, record := range h.records {
		if strings.Contains(record.Message, substr) {
			return true
		}
	}

	return false
}

// TestDedupLoopUpdatesStats feeds three envelopes (one duplicate)
// and asserts the dedupStats counters match: 3 seen, 1 dupe.
func TestDedupLoopUpdatesStats(t *testing.T) {
	t.Parallel()

	deduper := dedup.New(dedup.WithWindow(time.Second))
	framesCh := make(chan sourcemgr.Frame, 4)

	payloadA := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}
	payloadB := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x19}

	framesCh <- sourcemgr.Frame{Frame: beast.Frame{Bytes: payloadA}}

	framesCh <- sourcemgr.Frame{Frame: beast.Frame{Bytes: payloadA}} // duplicate

	framesCh <- sourcemgr.Frame{Frame: beast.Frame{Bytes: payloadB}}

	close(framesCh)

	stats := &dedupStats{}
	dedupLoop(t.Context(), framesCh, deduper, nil, nil, stats)

	if got := stats.seen.Load(); got != 3 {
		t.Errorf("stats.seen = %d, want 3", got)
	}

	if got := stats.dupes.Load(); got != 1 {
		t.Errorf("stats.dupes = %d, want 1", got)
	}
}

// TestSnapshotStatsCapturesSourceCounters checks that snapshotStats
// reads each source's address and counters via the public methods.
func TestSnapshotStatsCapturesSourceCounters(t *testing.T) {
	t.Parallel()

	sources := []*sourcemgr.Source{
		sourcemgr.New("upstream-a:30005"),
		sourcemgr.New("upstream-b:30005"),
	}

	stats := &dedupStats{}
	stats.seen.Store(100)
	stats.dupes.Store(40)

	snap := snapshotStats(sources, stats)

	if snap.seen != 100 || snap.dupes != 40 {
		t.Errorf("dedup snap = (seen=%d, dupes=%d), want (100, 40)", snap.seen, snap.dupes)
	}

	if len(snap.sources) != 2 {
		t.Fatalf("len(snap.sources) = %d, want 2", len(snap.sources))
	}

	if snap.sources[0].addr != "upstream-a:30005" || snap.sources[1].addr != "upstream-b:30005" {
		t.Errorf("snapshot addrs = %q + %q, want upstream-a:30005 + upstream-b:30005",
			snap.sources[0].addr, snap.sources[1].addr)
	}
}

// TestStatsSnapshotSubComputesDeltas verifies that statsSnapshot.sub
// returns per-source and dedup deltas (not cumulatives).
func TestStatsSnapshotSubComputesDeltas(t *testing.T) {
	t.Parallel()

	prior := statsSnapshot{
		seen:  100,
		dupes: 40,
		sources: []sourceSnapshot{
			{addr: "a", frames: 30, drops: 1},
			{addr: "b", frames: 70, drops: 5},
		},
	}

	current := statsSnapshot{
		seen:  150,
		dupes: 60,
		sources: []sourceSnapshot{
			{addr: "a", frames: 45, drops: 2},
			{addr: "b", frames: 100, drops: 7},
		},
	}

	delta := current.sub(prior)

	if delta.seen != 50 || delta.dupes != 20 {
		t.Errorf("dedup delta = (seen=%d, dupes=%d), want (50, 20)", delta.seen, delta.dupes)
	}

	wantSources := []sourceSnapshot{
		{addr: "a", frames: 15, drops: 1},
		{addr: "b", frames: 30, drops: 2},
	}

	if !reflect.DeepEqual(delta.sources, wantSources) {
		t.Errorf("source deltas = %+v, want %+v", delta.sources, wantSources)
	}
}

// TestStatsSnapshotSubHandlesNewSources covers the prior-shorter-
// than-current path: a new source appears mid-flight (shouldn't
// happen today, but the math must not underflow). The new source's
// delta is the full current count.
func TestStatsSnapshotSubHandlesNewSources(t *testing.T) {
	t.Parallel()

	prior := statsSnapshot{
		sources: []sourceSnapshot{{addr: "a", frames: 10, drops: 0}},
	}

	current := statsSnapshot{
		sources: []sourceSnapshot{
			{addr: "a", frames: 20, drops: 0},
			{addr: "b", frames: 5, drops: 0},
		},
	}

	delta := current.sub(prior)

	if delta.sources[1].frames != 5 {
		t.Errorf("new-source delta = %d, want 5 (full current count)", delta.sources[1].frames)
	}
}

// TestEmitStatsLogsAllAttrs captures the slog record and verifies
// each documented attribute is present.
func TestEmitStatsLogsAllAttrs(t *testing.T) {
	t.Parallel()

	handler := &captureHandler{}
	logger := slog.New(handler)

	delta := statsSnapshot{
		seen:  200,
		dupes: 50,
		sources: []sourceSnapshot{
			{addr: "upstream-a", frames: 120, drops: 1},
			{addr: "upstream-b", frames: 80, drops: 0},
		},
	}

	emitStats(t.Context(), logger, delta, 10*time.Second)

	handler.mu.Lock()
	defer handler.mu.Unlock()

	if len(handler.records) != 1 {
		t.Fatalf("emitStats wrote %d records, want 1", len(handler.records))
	}

	record := handler.records[0]
	if record.Message != "beastmux: stats" {
		t.Errorf("record.Message = %q, want %q", record.Message, "beastmux: stats")
	}

	want := map[string]bool{"interval": false, "seen": false, "dupes": false, "forwarded": false,
		"dedup_ratio": false, "upstream-a": false, "upstream-b": false}

	record.Attrs(func(attr slog.Attr) bool {
		if _, ok := want[attr.Key]; ok {
			want[attr.Key] = true
		}

		return true
	})

	for key, seen := range want {
		if !seen {
			t.Errorf("stats record missing attr %q", key)
		}
	}
}

// TestEmitStatsZeroSeenGivesZeroRatio is the guard on the
// `if delta.seen > 0` branch — when nothing arrived, the ratio is
// reported as 0 rather than NaN.
func TestEmitStatsZeroSeenGivesZeroRatio(t *testing.T) {
	t.Parallel()

	handler := &captureHandler{}

	emitStats(t.Context(), slog.New(handler), statsSnapshot{}, time.Second)

	handler.mu.Lock()
	defer handler.mu.Unlock()

	if len(handler.records) != 1 {
		t.Fatalf("emitStats wrote %d records, want 1", len(handler.records))
	}

	var ratio float64

	handler.records[0].Attrs(func(attr slog.Attr) bool {
		if attr.Key == "dedup_ratio" {
			ratio = attr.Value.Float64()
		}

		return true
	})

	if ratio != 0 {
		t.Errorf("dedup_ratio with zero seen = %v, want 0", ratio)
	}
}

// TestReportStatsTicksAndExits drives reportStats with a short
// interval, lets at least one tick fire, then cancels — the
// goroutine must emit a record and return cleanly.
func TestReportStatsTicksAndExits(t *testing.T) {
	t.Parallel()

	handler := &captureHandler{}
	logger := slog.New(handler)

	sources := []*sourcemgr.Source{sourcemgr.New("upstream-a")}
	stats := &dedupStats{}

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		reportStats(ctx, 10*time.Millisecond, sources, stats, logger)

		close(done)
	}()

	// Wait long enough for at least one tick to fire.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		handler.mu.Lock()
		count := len(handler.records)
		handler.mu.Unlock()

		if count > 0 {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reportStats did not exit within 1s of cancel")
	}

	handler.mu.Lock()
	defer handler.mu.Unlock()

	if len(handler.records) == 0 {
		t.Error("reportStats emitted no records before cancel")
	}
}

// TestDedupLoopWritesHexWhenEnabled checks the --stdout-hex emit
// path: each forwarded frame's payload is written as a lowercase
// hex line to the supplied writer.
func TestDedupLoopWritesHexWhenEnabled(t *testing.T) {
	t.Parallel()

	deduper := dedup.New(dedup.WithWindow(time.Second))
	framesCh := make(chan sourcemgr.Frame, 1)

	payload := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}

	framesCh <- sourcemgr.Frame{Source: "a", Frame: beast.Frame{Bytes: payload}}

	close(framesCh)

	var out bytes.Buffer

	dedupLoop(t.Context(), framesCh, deduper, nil, &out, &dedupStats{})

	want := "5d40621d423718\n"
	if got := out.String(); got != want {
		t.Errorf("hex out = %q, want %q", got, want)
	}
}
