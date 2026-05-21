package sourcemgr

import (
	"context"
	"fmt"
	"time"
)

// nextBackoff doubles current, capped at maxBackoff. The cap is
// applied after doubling so the cap value itself is reachable
// without ever exceeding it.
func nextBackoff(current, maxBackoff time.Duration) time.Duration {
	doubled := current * 2
	if doubled > maxBackoff {
		return maxBackoff
	}

	return doubled
}

// sleepCtx blocks for delay or until ctx is cancelled, whichever
// comes first. Returns nil on a clean wake, ctx.Err() on
// cancellation. A non-positive delay returns immediately.
func sleepCtx(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return fmt.Errorf("sourcemgr: sleep interrupted: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}
