package limiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew_InvalidConfig(t *testing.T) {
	if _, err := New(0, 10); err != ErrRateNegative {
		t.Fatalf("expected ErrRateNegative, got %v", err)
	}
	if _, err := New(-5, 10); err != ErrRateNegative {
		t.Fatalf("expected ErrRateNegative, got %v", err)
	}
	if _, err := New(100, 0); err != ErrBurstNegative {
		t.Fatalf("expected ErrBurstNegative, got %v", err)
	}
	if _, err := New(100, -1); err != ErrBurstNegative {
		t.Fatalf("expected ErrBurstNegative, got %v", err)
	}
}

func TestLimiter_Burst(t *testing.T) {
	lim, err := New(10, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 5; i++ {
		if !lim.Allow() {
			t.Fatalf("expected token %d to be allowed under burst capacity", i)
		}
	}

	if lim.Allow() {
		t.Fatal("expected 6th token to be rejected when burst is exhausted")
	}
}

func TestLimiter_Refill(t *testing.T) {
	lim, err := New(100, 10) // 100 tokens/sec -> 1 token every 10ms
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 10; i++ {
		if !lim.Allow() {
			t.Fatalf("failed to consume initial token %d", i)
		}
	}

	if lim.Allow() {
		t.Fatal("bucket should be empty")
	}

	time.Sleep(35 * time.Millisecond) // should generate ~3 tokens

	consumed := 0
	for i := 0; i < 5; i++ {
		if lim.Allow() {
			consumed++
		}
	}

	if consumed < 2 || consumed > 4 {
		t.Fatalf("expected between 2 and 4 refilled tokens, got %d", consumed)
	}
}

func TestLimiter_Wait_ContextCancellation(t *testing.T) {
	lim, err := New(1, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !lim.Allow() {
		t.Fatal("first token should pass")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err = lim.Wait(ctx, 1)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestLimiter_ConcurrentAccess_RaceDetector(t *testing.T) {
	const (
		rate       = 10_000
		burst      = 500
		goroutines = 32
		iterations = 1_000
	)

	lim, err := New(rate, burst)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var (
		wg      sync.WaitGroup
		allowed atomic.Int64
		denied  atomic.Int64
	)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if lim.Allow() {
					allowed.Add(1)
				} else {
					denied.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	total := allowed.Load() + denied.Load()
	expectedTotal := int64(goroutines * iterations)
	if total != expectedTotal {
		t.Fatalf("expected total attempts %d, got %d", expectedTotal, total)
	}

	if allowed.Load() == 0 {
		t.Fatal("expected at least burst tokens to be allowed")
	}
}

func BenchmarkLimiter_Allow(b *testing.B) {
	lim, err := New(100_000_000, 100_000_000)
	if err != nil {
		b.Fatalf("failed to create limiter: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = lim.Allow()
	}
}

func BenchmarkLimiter_AllowParallel(b *testing.B) {
	lim, err := New(100_000_000, 100_000_000)
	if err != nil {
		b.Fatalf("failed to create limiter: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = lim.Allow()
		}
	})
}
