//go:build integration

package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/demod1090/beast"
	"github.com/hyperized/demod1090/beastsrv"
)

// fakeUpstream is a minimal BEAST source for the integration test:
// listens on a loopback port, accepts one client at a time, and
// exposes that client so the test can write encoded frames into it.
type fakeUpstream struct {
	listener net.Listener
	mu       sync.Mutex
	conn     net.Conn
	accepted chan struct{}
}

func startFakeUpstream(t *testing.T) *fakeUpstream {
	t.Helper()

	listenCfg := &net.ListenConfig{}

	listener, err := listenCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	upstream := &fakeUpstream{
		listener: listener,
		accepted: make(chan struct{}, 1),
	}

	go upstream.acceptLoop()

	t.Cleanup(func() { _ = upstream.Close() })

	return upstream
}

func (f *fakeUpstream) Addr() string { return f.listener.Addr().String() }

func (f *fakeUpstream) WaitForClient(t *testing.T) {
	t.Helper()

	select {
	case <-f.accepted:
	case <-time.After(2 * time.Second):
		t.Fatalf("upstream %s: no client connected within 2s", f.Addr())
	}
}

func (f *fakeUpstream) Send(t *testing.T, wire []byte) {
	t.Helper()

	f.mu.Lock()
	conn := f.conn
	f.mu.Unlock()

	if conn == nil {
		t.Fatal("fakeUpstream.Send: no client connected")
	}

	if _, err := conn.Write(wire); err != nil {
		t.Fatalf("fakeUpstream.Send: %v", err)
	}
}

func (f *fakeUpstream) Close() error {
	_ = f.listener.Close()

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.conn != nil {
		_ = f.conn.Close()
		f.conn = nil
	}

	return nil
}

func (f *fakeUpstream) acceptLoop() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}

		f.mu.Lock()

		if f.conn != nil {
			_ = f.conn.Close()
		}

		f.conn = conn

		f.mu.Unlock()

		select {
		case f.accepted <- struct{}{}:
		default:
		}
	}
}

// TestEndToEndDedupes spins up two upstreams + the full beastmux
// wiring, sends overlapping frames into each upstream, dials the
// consolidated output, and asserts exactly the unique frames come
// out.
func TestEndToEndDedupes(t *testing.T) {
	t.Parallel()

	rig := startEndToEndRig(t)
	defer rig.cancel()

	payloadShared := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}
	payloadAOnly := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x19}
	payloadBOnly := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x1A}

	// Shared frame from both — dedup should keep exactly one.
	rig.upstreamA.Send(t, mustEncode(t, payloadShared, 1, 1))
	rig.upstreamB.Send(t, mustEncode(t, payloadShared, 1, 1))

	// Unique frames from each — both should propagate.
	rig.upstreamA.Send(t, mustEncode(t, payloadAOnly, 2, 2))
	rig.upstreamB.Send(t, mustEncode(t, payloadBOnly, 3, 3))

	received := readFrames(t, rig.conn, 3, time.Second)
	if len(received) != 3 {
		t.Fatalf("received %d frames, want 3", len(received))
	}

	want := [][]byte{payloadShared, payloadAOnly, payloadBOnly}
	if !sameSet(received, want) {
		t.Errorf("received payloads = %x, want set %x", received, want)
	}

	// A fourth read must time out — anything more means a duplicate.
	if extras := readFrames(t, rig.conn, 1, 200*time.Millisecond); len(extras) != 0 {
		t.Errorf("received %d extra frames, want 0 (dedup leaked)", len(extras))
	}

	rig.shutdown(t)
}

// endToEndRig holds the wired-up state for an integration test —
// two upstreams, the beastmux server, an open consumer conn, and
// the cancellation handle that tears it all down. Keeps individual
// tests under revive's funlen limit by isolating the setup churn.
type endToEndRig struct {
	upstreamA *fakeUpstream
	upstreamB *fakeUpstream
	server    *beastsrv.Server
	conn      net.Conn
	cancel    context.CancelFunc
	runDone   <-chan error
}

func startEndToEndRig(t *testing.T) *endToEndRig {
	t.Helper()

	upstreamA := startFakeUpstream(t)
	upstreamB := startFakeUpstream(t)

	server := beastsrv.NewServer(
		beastsrv.WithListenAddr("127.0.0.1:0"),
		beastsrv.WithLogger(discardLogger()),
	)

	cfg := config{
		sources:      []string{upstreamA.Addr(), upstreamB.Addr()},
		dedupWindow:  500 * time.Millisecond,
		reconnectMin: 5 * time.Millisecond,
		reconnectMax: 50 * time.Millisecond,
		server:       server,
	}

	ctx, cancel := context.WithCancel(t.Context())

	runDone := make(chan error, 1)

	go func() { runDone <- run(ctx, cfg, discardLogger()) }()

	waitForBind(t, server)

	conn, err := dialServer(t, server)
	if err != nil {
		cancel()
		t.Fatalf("dial server: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	waitForClient(t, server, 1)
	upstreamA.WaitForClient(t)
	upstreamB.WaitForClient(t)

	return &endToEndRig{
		upstreamA: upstreamA,
		upstreamB: upstreamB,
		server:    server,
		conn:      conn,
		cancel:    cancel,
		runDone:   runDone,
	}
}

func (r *endToEndRig) shutdown(t *testing.T) {
	t.Helper()

	r.cancel()

	select {
	case err := <-r.runDone:
		if err != nil {
			t.Errorf("run returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not exit within 2s of ctx cancel")
	}
}

func mustEncode(t *testing.T, payload []byte, ticks uint64, signal byte) []byte {
	t.Helper()

	wire, err := beast.Encode(nil, payload, ticks, signal)
	if err != nil {
		t.Fatalf("beast.Encode: %v", err)
	}

	return wire
}
