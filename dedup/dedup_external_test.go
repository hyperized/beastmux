package dedup_test

import (
	"sync"
	"testing"
	"time"

	"github.com/hyperized/beastmux/dedup"
)

// TestDedupIdenticalPayloadsViaKey is the canonical use case:
// hash a payload with Key, feed it to Seen — the second occurrence
// inside the window reports as a duplicate.
func TestDedupIdenticalPayloadsViaKey(t *testing.T) {
	t.Parallel()

	deduper := dedup.New()
	payload := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}
	now := time.Now()

	first := deduper.Seen(dedup.Key(payload), now)
	if first {
		t.Fatal("first Seen() of payload = true, want false (new key)")
	}

	second := deduper.Seen(dedup.Key(payload), now.Add(50*time.Millisecond))
	if !second {
		t.Error("second Seen() of identical payload = false, want true (duplicate)")
	}
}

// TestDedupDistinctPayloadsAllForwarded confirms two byte-different
// payloads hash to different keys and both report as new.
func TestDedupDistinctPayloadsAllForwarded(t *testing.T) {
	t.Parallel()

	deduper := dedup.New()
	now := time.Now()

	one := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x18}
	two := []byte{0x5D, 0x40, 0x62, 0x1D, 0x42, 0x37, 0x19} // one bit flipped

	if deduper.Seen(dedup.Key(one), now) {
		t.Error("payload one reported as duplicate on first sight")
	}

	if deduper.Seen(dedup.Key(two), now) {
		t.Error("payload two reported as duplicate on first sight (distinct payloads should hash differently)")
	}

	if deduper.Size() != 2 {
		t.Errorf("Size() = %d, want 2 after two distinct payloads", deduper.Size())
	}
}

// TestSeenConcurrentAccessSafe drives Seen and Size from many
// goroutines under -race. Pure smoke under the race detector; the
// assertion is just "no panics, no race report, no lost entries
// relative to a known unique-key set".
func TestSeenConcurrentAccessSafe(t *testing.T) {
	t.Parallel()

	const (
		workers       uint64 = 16
		keysPerWorker uint64 = 256
	)

	deduper := dedup.New(dedup.WithWindow(time.Hour))
	now := time.Now()

	var waitGroup sync.WaitGroup

	for worker := range workers {
		waitGroup.Add(1)

		go func(base uint64) {
			defer waitGroup.Done()

			for offset := range keysPerWorker {
				_ = deduper.Seen(base*keysPerWorker+offset, now)
			}
		}(worker)
	}

	waitGroup.Wait()

	if got, want := deduper.Size(), int(workers*keysPerWorker); got != want {
		t.Errorf("Size() = %d, want %d (each goroutine inserted a unique key range)", got, want)
	}
}
