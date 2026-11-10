package limiter

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrRateNegative  = errors.New("ringlimit: rate must be greater than zero")
	ErrBurstNegative = errors.New("ringlimit: burst must be greater than zero")
	ErrExceedsBurst  = errors.New("ringlimit: requested tokens exceed maximum burst capacity")
)

type Limiter struct {
	// 64-bit alignment and 64-byte cache-line padding to avoid false sharing on SMP cores
	state uint64
	_     [56]byte

	ratePerSec uint64
	burst      uint64
	intervalNs uint64
}

// Bit layout of state uint64:
// [63:32] tokens (uint32)
// [31:0]  last tick timestamp in nanoseconds truncated or epoch offset
// For high resolution and long runtime, we store:
// - atomic state word packed as:
//   tokens: uint32 in high 32 bits (fixed point / whole tokens)
//   lastUpdateMicros: uint32 in low 32 bits (wraps every ~71 minutes, handled via modulo delta)
//
// Alternatively, to avoid 32-bit timestamp wrap-around ambiguity under heavy load,
// we use two dedicated 64-bit atomic values placed on separate cache lines.

type CachePaddedLimiter struct {
	// tokens stored as fixed-point value scaled by 1,000,000 for sub-token precision
	tokens atomic.Int64
	_      [56]byte

	lastUpdate atomic.Int64
	_          [56]byte

	ratePerSec int64 // tokens per second
	burst      int64 // max token capacity
	maxScaled  int64 // burst * scaleFactor
	scale      int64 // 1e6 multiplier
}

const tokenScale = 1_000_000

func New(ratePerSec, burst int64) (*CachePaddedLimiter, error) {
	if ratePerSec <= 0 {
		return nil, ErrRateNegative
	}
	if burst <= 0 {
		return nil, ErrBurstNegative
	}

	lim := &CachePaddedLimiter{
		ratePerSec: ratePerSec,
		burst:      burst,
		scale:      tokenScale,
		maxScaled:  burst * tokenScale,
	}

	lim.tokens.Store(lim.maxScaled)
	lim.lastUpdate.Store(time.Now().UnixNano())

	return lim, nil
}

// Allow reports whether 1 token is available and consumes it.
// Fast-path: non-blocking, zero-allocation, atomic CAS.
func (l *CachePaddedLimiter) Allow() bool {
	return l.Take(1)
}

// Take attempts to take n tokens from the bucket.
// Returns true if consumed, false if insufficient tokens.
func (l *CachePaddedLimiter) Take(n int64) bool {
	if n <= 0 {
		return true
	}
	if n > l.burst {
		return false
	}

	requestedScaled := n * l.scale

	for {
		now := time.Now().UnixNano()
		last := l.lastUpdate.Load()

		currentTokens := l.tokens.Load()
		elapsed := now - last

		refilledTokens := currentTokens
		if elapsed > 0 {
			// Calculate refilled tokens based on elapsed nanoseconds
			// tokens = elapsed * rate / 1e9
			refill := (elapsed * l.ratePerSec * l.scale) / 1_000_000_000
			if refill > 0 {
				refilledTokens += refill
				if refilledTokens > l.maxScaled {
					refilledTokens = l.maxScaled
				}
			}
		}

		if refilledTokens < requestedScaled {
			return false
		}

		newTokens := refilledTokens - requestedScaled

		// Atomic compare-and-swap for lock-free state progression
		if l.tokens.CompareAndSwap(currentTokens, newTokens) {
			if elapsed > 0 {
				l.lastUpdate.Store(now)
			}
			return true
		}
		// CAS failed due to concurrent contention; retry loop immediately
	}
}

// Wait blocks until n tokens become available or the context is cancelled.
func (l *CachePaddedLimiter) Wait(ctx context.Context, n int64) error {
	if n > l.burst {
		return ErrExceedsBurst
	}

	for {
		if l.Take(n) {
			return nil
		}

		// Calculate sleep time until next token arrives
		missing := (n * l.scale) - l.tokens.Load()
		if missing <= 0 {
			missing = l.scale
		}

		delayNs := (missing * 1_000_000_000) / (l.ratePerSec * l.scale)
		if delayNs < 50_000 {
			delayNs = 50_000 // minimum 50µs clamp to avoid busy-spin
		}

		timer := time.NewTimer(time.Duration(delayNs))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Available returns the current number of available whole tokens.
func (l *CachePaddedLimiter) Available() int64 {
	tokens := l.tokens.Load()
	if tokens <= 0 {
		return 0
	}
	return tokens / l.scale
}

// Capacity returns the maximum configured burst capacity.
func (l *CachePaddedLimiter) Capacity() int64 {
	return l.burst
}

// Rate returns the configured refill rate per second.
func (l *CachePaddedLimiter) Rate() int64 {
	return l.ratePerSec
}
