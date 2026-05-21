package dedup

import (
	"testing"
	"time"
)

func TestNewDefaults(t *testing.T) {
	t.Parallel()

	dedup := New()

	if got := dedup.Window(); got != DefaultWindow {
		t.Errorf("Window() = %v, want %v", got, DefaultWindow)
	}

	if got := dedup.MaxSize(); got != DefaultMaxSize {
		t.Errorf("MaxSize() = %d, want %d", got, DefaultMaxSize)
	}

	if got := dedup.Size(); got != 0 {
		t.Errorf("Size() = %d, want 0 (fresh dedup)", got)
	}
}

func TestWithWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give time.Duration
		want time.Duration
	}{
		{name: "custom", give: 5 * time.Second, want: 5 * time.Second},
		{name: "zero ignored", give: 0, want: DefaultWindow},
		{name: "negative ignored", give: -1 * time.Second, want: DefaultWindow},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			dedup := New(WithWindow(testCase.give))
			if got := dedup.Window(); got != testCase.want {
				t.Errorf("Window() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestWithMaxSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give int
		want int
	}{
		{name: "custom", give: 128, want: 128},
		{name: "zero ignored", give: 0, want: DefaultMaxSize},
		{name: "negative ignored", give: -7, want: DefaultMaxSize},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			dedup := New(WithMaxSize(testCase.give))
			if got := dedup.MaxSize(); got != testCase.want {
				t.Errorf("MaxSize() = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestSeenNewKeyReturnsFalse(t *testing.T) {
	t.Parallel()

	dedup := New()

	if dedup.Seen(0xABCD, time.Now()) {
		t.Error("first Seen() = true, want false (new key)")
	}

	if dedup.Size() != 1 {
		t.Errorf("Size() = %d, want 1 after first Seen", dedup.Size())
	}
}

func TestSeenDuplicateWithinWindowReturnsTrue(t *testing.T) {
	t.Parallel()

	dedup := New(WithWindow(500 * time.Millisecond))
	now := time.Now()

	if dedup.Seen(0xABCD, now) {
		t.Fatal("first Seen() = true, want false (new key)")
	}

	if !dedup.Seen(0xABCD, now.Add(100*time.Millisecond)) {
		t.Error("second Seen() within window = false, want true (duplicate)")
	}
}

func TestSeenExpiredKeyTreatedAsNew(t *testing.T) {
	t.Parallel()

	dedup := New(WithWindow(500 * time.Millisecond))
	now := time.Now()

	if dedup.Seen(0xABCD, now) {
		t.Fatal("first Seen() = true, want false")
	}

	// 600 ms later — past the 500 ms window.
	if dedup.Seen(0xABCD, now.Add(600*time.Millisecond)) {
		t.Error("Seen() past window = true, want false (entry should have expired)")
	}
}

func TestSeenRefreshExtendsWindow(t *testing.T) {
	t.Parallel()

	dedup := New(WithWindow(500 * time.Millisecond))
	now := time.Now()

	// Seed at t=0, refresh at t=300 ms. The second call refreshes
	// the expiry to 300+500 = 800 ms. A probe at t=600 ms is past
	// the original 500 ms expiry but inside the refreshed window.
	if dedup.Seen(0xABCD, now) {
		t.Fatal("initial Seen() = true, want false")
	}

	if !dedup.Seen(0xABCD, now.Add(300*time.Millisecond)) {
		t.Fatal("refresh Seen() = false, want true")
	}

	if !dedup.Seen(0xABCD, now.Add(600*time.Millisecond)) {
		t.Error("Seen() after refresh = false; refresh did not extend window")
	}
}

// TestSeenEvictionTriggeredAtCap forces the map past MaxSize with
// entries whose TTL has already elapsed; the opportunistic sweep
// inside Seen must clear them.
func TestSeenEvictionTriggeredAtCap(t *testing.T) {
	t.Parallel()

	dedup := New(WithWindow(10*time.Millisecond), WithMaxSize(2))
	now := time.Now()

	dedup.Seen(1, now)
	dedup.Seen(2, now)

	if dedup.Size() != 2 {
		t.Fatalf("Size() = %d, want 2 before sweep", dedup.Size())
	}

	// Third insert past the window — sweep should clear keys 1+2.
	dedup.Seen(3, now.Add(50*time.Millisecond))

	if dedup.Size() != 1 {
		t.Errorf("Size() = %d, want 1 after sweep (keys 1+2 expired)", dedup.Size())
	}
}

// TestSeenEvictionNoOpWhenNoneExpired exercises the
// "cap exceeded but nothing is stale" branch — the map is allowed
// to grow past MaxSize rather than drop the new frame.
func TestSeenEvictionNoOpWhenNoneExpired(t *testing.T) {
	t.Parallel()

	dedup := New(WithWindow(time.Hour), WithMaxSize(2))
	now := time.Now()

	dedup.Seen(1, now)
	dedup.Seen(2, now)
	dedup.Seen(3, now) // triggers the sweep, but everything is in-window

	if dedup.Size() != 3 {
		t.Errorf("Size() = %d, want 3 (cap is a sweep trigger, not a hard limit)", dedup.Size())
	}
}

func TestEvictExpiredRemovesOnlyExpired(t *testing.T) {
	t.Parallel()

	dedup := New(WithWindow(500 * time.Millisecond))
	now := time.Now()

	dedup.Seen(1, now)
	dedup.Seen(2, now.Add(300*time.Millisecond))

	// Sweep at t=600 ms: key 1 expired (expiry 500 ms), key 2 still
	// alive (expiry 800 ms).
	dedup.mu.Lock()
	dedup.evictExpired(now.Add(600 * time.Millisecond))
	dedup.mu.Unlock()

	if dedup.Size() != 1 {
		t.Fatalf("Size() = %d, want 1 after partial eviction", dedup.Size())
	}

	if !dedup.Seen(2, now.Add(600*time.Millisecond)) {
		t.Error("key 2 evicted prematurely; expected still in-window")
	}
}

func TestKeyDeterministic(t *testing.T) {
	t.Parallel()

	payload := []byte{0x88, 0x40, 0x62, 0x1D, 0x58, 0xC3, 0x82, 0xD6, 0x90, 0xC8, 0xAC, 0x28, 0x63, 0xA7}

	first := Key(payload)
	second := Key(payload)

	if first != second {
		t.Errorf("Key() not deterministic within the same process: %x vs %x", first, second)
	}
}

func TestKeyDifferentPayloadsDifferentHashes(t *testing.T) {
	t.Parallel()

	left := []byte{0x88, 0x40, 0x62, 0x1D, 0x58, 0xC3, 0x82}
	right := []byte{0x88, 0x40, 0x62, 0x1D, 0x58, 0xC3, 0x83} // one bit flipped

	if Key(left) == Key(right) {
		t.Error("Key() returned identical hashes for distinct payloads — vanishingly unlikely with maphash")
	}
}
