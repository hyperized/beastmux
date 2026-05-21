package main

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hyperized/demod1090/beast"
	"github.com/hyperized/demod1090/beastsrv"
)

const (
	bindPollInterval = 5 * time.Millisecond
	bindPollMax      = 2 * time.Second
)

// waitForBind blocks until server.Addr() returns non-nil or the test
// deadline passes; t.Fatal fires on timeout.
func waitForBind(t *testing.T, server *beastsrv.Server) {
	t.Helper()

	deadline := time.Now().Add(bindPollMax)
	for time.Now().Before(deadline) {
		if server.Addr() != nil {
			return
		}

		time.Sleep(bindPollInterval)
	}

	t.Fatal("server did not bind within deadline")
}

// dialServer dials the server's listener address. The caller is
// responsible for closing the returned conn.
func dialServer(t *testing.T, server *beastsrv.Server) (net.Conn, error) {
	t.Helper()

	addr := server.Addr()
	if addr == nil {
		return nil, errServerNotBound
	}

	dialer := &net.Dialer{Timeout: time.Second}

	conn, err := dialer.DialContext(t.Context(), addr.Network(), addr.String())
	if err != nil {
		return nil, err //nolint:wrapcheck // test helper; surface raw net error
	}

	return conn, nil
}

var errServerNotBound = errors.New("server is not bound")

// waitForClient blocks until the server reports at least minClients
// connected; t.Fatal fires on timeout.
func waitForClient(t *testing.T, server *beastsrv.Server, minClients int) {
	t.Helper()

	deadline := time.Now().Add(bindPollMax)
	for time.Now().Before(deadline) {
		if server.ClientCount() >= minClients {
			return
		}

		time.Sleep(bindPollInterval)
	}

	t.Fatalf("server did not accept %d client(s) within deadline (got %d)",
		minClients, server.ClientCount())
}

// readFrames reads up to max frames from r with a total deadline.
// Returns whatever was read; the caller decides whether short reads
// are a failure.
func readFrames(t *testing.T, conn net.Conn, maxFrames int, deadline time.Duration) [][]byte {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(deadline)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	reader := beast.NewReader(conn)

	frames := make([][]byte, 0, maxFrames)

	for range maxFrames {
		frame, err := reader.Frame()
		if err != nil {
			// Read deadline / EOF — stop reading, return what we have.
			if errors.Is(err, io.EOF) || isTimeout(err) {
				return frames
			}

			return frames
		}

		frames = append(frames, frame.Bytes)
	}

	return frames
}

// isTimeout reports whether err is a net.Error indicating a timeout.
func isTimeout(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	return false
}

// sameSet reports whether got and want contain the same byte slices,
// ignoring order. Used to check dedup output where the upstream
// arrival order is non-deterministic.
func sameSet(got, want [][]byte) bool {
	if len(got) != len(want) {
		return false
	}

	matched := make([]bool, len(want))

	for _, candidate := range got {
		found := false

		for index, target := range want {
			if !matched[index] && bytes.Equal(candidate, target) {
				matched[index] = true
				found = true

				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}
