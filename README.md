# ringlimit

Lock-free, cache-line aligned token bucket rate limiter and backpressure shaper for Go.

Designed for high-throughput packet processing, API gateways, and distributed message queues where mutex contention causes thread parking and CPU cache misses.

```
                  +-----------------------------------+
                  |        SMP CPU Core Cache         |
                  +-----------------------------------+
                   |                                 |
         Line 0 (64 Bytes)                 Line 1 (64 Bytes)
  +------------------------------+  +------------------------------+
  |  tokens: atomic.Int64 (8B)   |  | lastUpdate: atomic.Int64(8B) |
  |  _pad:   [56]byte            |  | _pad:       [56]byte         |
  +------------------------------+  +------------------------------+
                   \                                 /
                    \------ Lock-Free CAS Loop -----/
                                    |
                            [ Allow() / Take() ]
                                    |
                           Zero Allocations (0 B/op)
```

## Features

- **Lock-Free Atomic CAS:** Uses single-word 64-bit atomic operations instead of sync.Mutex.
- **Cache-Line Padding:** Prevents false sharing across multicore processors by isolating atomic variables onto dedicated 64-byte L1 cache boundaries.
- **Zero Allocations:** Evaluates `Allow()` and `Take(n)` without allocating memory on the Go heap (`0 B/op, 0 allocs/op`).
- **Lazy Refill Calculation:** No background tick goroutines or timer leaks. Tokens are refilled lazily on-demand using nanosecond-precision deltas.
- **Sub-Token Precision:** Uses fixed-point scaling factor (`1,000,000x`) to eliminate integer truncation artifacts at high rates.

## Benchmarks

Benchmarked on AMD Ryzen 9 6900HX (16 threads) Linux 6.x:

```
goos: linux
goarch: amd64
pkg: github.com/cicekhasann/ringlimit/pkg/limiter
cpu: AMD Ryzen 9 6900HX with Radeon Graphics
BenchmarkLimiter_Allow-16            26806790         44.48 ns/op        0 B/op        0 allocs/op
BenchmarkLimiter_AllowParallel-16    32820567         33.65 ns/op        0 B/op        0 allocs/op
```

## Usage

```go
package main

import (
	"fmt"
	"github.com/cicekhasann/ringlimit/pkg/limiter"
)

func main() {
	// 50,000 tokens/sec with burst of 10,000
	lim, err := limiter.New(50000, 10000)
	if err != nil {
		panic(err)
	}

	if lim.Allow() {
		// handle request
	} else {
		// drop or return 429 Too Many Requests
	}
}
```

### Context Cancellation / Blocking Wait

```go
ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
defer cancel()

if err := lim.Wait(ctx, 5); err != nil {
	// context deadline exceeded or canceled
}
```

## Building & Testing

```bash
make test    # Run unit tests
make race    # Run race detector (go test -race)
make bench   # Run memory allocation & ns/op benchmarks
make build   # Compile static binary to bin/ringlimit
```

## License

MIT License. See [LICENSE](LICENSE).
