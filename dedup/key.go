package dedup

import "hash/maphash"

// keySeed is the process-wide randomised seed used by Key. Drawn
// once at package init so all Dedup instances in the process share
// it (callers can hash with Key and look up with Seen). The seed is
// fresh per process start, which makes adversarial collision-
// forcing infeasible without also picking the seed.
//
//nolint:gochecknoglobals // a process-wide hash seed is the whole point — it must be a single value all callers see.
var keySeed = maphash.MakeSeed()

// Key returns a 64-bit hash of payload suitable for Seen. payload
// is intended to be the raw Mode S frame bytes (beast.Frame.Bytes
// in the demod1090 wire model) — the BEAST envelope's timestamp
// and signal-level fields must be excluded so two receivers seeing
// the same on-air frame produce the same key.
//
// The hash is randomised per process; callers must not persist Key
// results across runs or compare them against another process's
// keys.
func Key(payload []byte) uint64 {
	return maphash.Bytes(keySeed, payload)
}
