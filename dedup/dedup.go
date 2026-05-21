package dedup

import (
	"sync"
	"time"
)

// DefaultWindow is the TTL applied to each key. 500 ms covers the
// worst-case publish + network jitter between two receivers seeing
// the same on-air frame while staying short enough that a genuine
// retransmission of the same payload seconds later registers as
// new.
const DefaultWindow = 500 * time.Millisecond

// DefaultMaxSize is the entry count that triggers an opportunistic
// sweep inside Seen. At 10 K frames/sec with the default window
// the steady state is ~5 K live entries; 65536 leaves 6.5×
// headroom before the sweep kicks in.
const DefaultMaxSize = 65536

// Dedup is a TTL'd set of 64-bit keys. The zero value is unusable;
// construct via New. Seen is safe for concurrent use; a single
// mutex guards the underlying map.
type Dedup struct {
	mu      sync.Mutex
	window  time.Duration
	maxSize int
	entries map[uint64]time.Time
}

// Option configures a Dedup at construction.
type Option func(*Dedup)

// WithWindow overrides DefaultWindow. Values ≤ 0 are ignored — the
// default stays in place.
func WithWindow(window time.Duration) Option {
	return func(d *Dedup) {
		if window > 0 {
			d.window = window
		}
	}
}

// WithMaxSize overrides DefaultMaxSize. Values ≤ 0 are ignored —
// the default stays in place.
func WithMaxSize(maxSize int) Option {
	return func(d *Dedup) {
		if maxSize > 0 {
			d.maxSize = maxSize
		}
	}
}

// New returns an empty Dedup with the supplied options applied.
func New(opts ...Option) *Dedup {
	dedup := &Dedup{
		window:  DefaultWindow,
		maxSize: DefaultMaxSize,
		entries: make(map[uint64]time.Time),
	}

	for _, opt := range opts {
		opt(dedup)
	}

	return dedup
}

// Window reports the TTL the Dedup was constructed with.
func (d *Dedup) Window() time.Duration {
	return d.window
}

// MaxSize reports the entry count that triggers an opportunistic
// sweep inside Seen.
func (d *Dedup) MaxSize() int {
	return d.maxSize
}

// Size returns the current number of tracked entries. Snapshot
// only: the value may already be stale by the time the caller
// reads it.
func (d *Dedup) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return len(d.entries)
}

// Seen records key with an expiry of now+Window and reports
// whether the key was already in-flight (true = duplicate, drop).
// A duplicate hit refreshes the entry so the next copy in the same
// burst is still recognised as a dupe.
//
// When the map exceeds MaxSize, Seen sweeps expired entries
// opportunistically. The sweep is best-effort; if every entry is
// still in-window the map is allowed to grow past MaxSize rather
// than discard the new frame.
func (d *Dedup) Seen(key uint64, now time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	expiry, present := d.entries[key]
	if present && now.Before(expiry) {
		d.entries[key] = now.Add(d.window)

		return true
	}

	d.entries[key] = now.Add(d.window)

	if len(d.entries) > d.maxSize {
		d.evictExpired(now)
	}

	return false
}

// evictExpired removes entries whose expiry is at or before now.
// Caller must hold d.mu.
func (d *Dedup) evictExpired(now time.Time) {
	for key, expiry := range d.entries {
		if !now.Before(expiry) {
			delete(d.entries, key)
		}
	}
}
