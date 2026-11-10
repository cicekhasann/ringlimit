package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cicekhasann/ringlimit/pkg/limiter"
)

type Stats struct {
	Allowed   int64   `json:"allowed"`
	Denied    int64   `json:"denied"`
	Total     int64   `json:"total"`
	Throughput float64 `json:"throughput_ops_sec"`
	Available int64   `json:"tokens_available"`
	Capacity  int64   `json:"burst_capacity"`
	Rate      int64   `json:"rate_per_sec"`
}

var (
	allowedCount atomic.Int64
	deniedCount  atomic.Int64
	globalLim    *limiter.CachePaddedLimiter
)

func main() {
	rate := flag.Int64("rate", 1_000_000, "Refill rate (tokens per second)")
	burst := flag.Int64("burst", 500_000, "Maximum burst capacity")
	workers := flag.Int("workers", 8, "Number of concurrent worker goroutines for stress test")
	duration := flag.Duration("duration", 0, "Stress test duration (0 for indefinite run)")
	httpAddr := flag.String("http", "", "Serve live metrics and visual sandbox on HTTP address (e.g. :8081)")
	flag.Parse()

	lim, err := limiter.New(*rate, *burst)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ringlimit: initialization failed: %v\n", err)
		os.Exit(1)
	}
	globalLim = lim

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	if *httpAddr != "" {
		go startHTTPServer(*httpAddr)
	}

	fmt.Printf("\033[1;36m=== ringlimit // High-Throughput Token Bucket Engine ===\033[0m\n")
	fmt.Printf("Rate: %d tokens/sec | Burst: %d | Workers: %d\n", *rate, *burst, *workers)
	if *httpAddr != "" {
		fmt.Printf("HTTP live telemetry: http://localhost%s\n", *httpAddr)
	}
	fmt.Printf("Running stress benchmark... Press Ctrl+C to terminate.\n\n")

	var wg sync.WaitGroup
	wg.Add(*workers)
	for i := 0; i < *workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if lim.Allow() {
						allowedCount.Add(1)
					} else {
						deniedCount.Add(1)
					}
				}
			}
		}()
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastAllowed, lastDenied int64
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			elapsed := time.Since(start).Seconds()
			totalAllowed := allowedCount.Load()
			totalDenied := deniedCount.Load()
			totalReqs := totalAllowed + totalDenied

			fmt.Printf("\n\033[1;32m--- Benchmark Completed ---\033[0m\n")
			fmt.Printf("Total Elapsed:   %.2fs\n", elapsed)
			fmt.Printf("Allowed Reqs:    %d (%.2f req/s)\n", totalAllowed, float64(totalAllowed)/elapsed)
			fmt.Printf("Denied Reqs:     %d (%.2f req/s)\n", totalDenied, float64(totalDenied)/elapsed)
			fmt.Printf("Total Evaluated: %d (%.2f req/s)\n", totalReqs, float64(totalReqs)/elapsed)
			return

		case <-ticker.C:
			currAllowed := allowedCount.Load()
			currDenied := deniedCount.Load()

			deltaAllowed := currAllowed - lastAllowed
			deltaDenied := currDenied - lastDenied
			deltaTotal := deltaAllowed + deltaDenied

			lastAllowed = currAllowed
			lastDenied = currDenied

			fmt.Printf("\r\033[K[t+%.0fs] \033[32mPASS: %10d/s\033[0m | \033[31mDROP: %10d/s\033[0m | \033[36mTHROUGHPUT: %10d ops/s\033[0m | TOKENS: %d",
				time.Since(start).Seconds(),
				deltaAllowed,
				deltaDenied,
				deltaTotal,
				lim.Available(),
			)
		}
	}
}

func startHTTPServer(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		allowed := allowedCount.Load()
		denied := deniedCount.Load()

		stats := Stats{
			Allowed:   allowed,
			Denied:    denied,
			Total:     allowed + denied,
			Available: globalLim.Available(),
			Capacity:  globalLim.Capacity(),
			Rate:      globalLim.Rate(),
		}
		_ = json.NewEncoder(w).Encode(stats)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(htmlSandbox))
	})

	_ = http.ListenAndServe(addr, mux)
}

const htmlSandbox = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>ringlimit // Live Telemetry & Sandbox</title>
  <style>
    body { font-family: monospace; background: #0a0c10; color: #e2e8f0; padding: 2rem; }
    h1 { color: #38bdf8; font-size: 1.4rem; margin-bottom: 0.5rem; }
    .card { background: #111827; border: 1px solid #1f2937; border-radius: 8px; padding: 1.5rem; max-width: 680px; }
    .metric { display: flex; justify-content: space-between; padding: 0.5rem 0; border-bottom: 1px solid #1e293b; }
    .val { font-weight: bold; color: #4ade80; }
    .val-denied { color: #f87171; }
  </style>
</head>
<body>
  <div class="card">
    <h1>ringlimit: Lock-Free Rate Limiter</h1>
    <p>Live metrics polled from local kernel worker threads.</p>
    <div class="metric"><span>Tokens Available</span><span class="val" id="tokens">-</span></div>
    <div class="metric"><span>Refill Rate</span><span class="val" id="rate">-</span></div>
    <div class="metric"><span>Burst Capacity</span><span class="val" id="burst">-</span></div>
    <div class="metric"><span>Total Passed</span><span class="val" id="allowed">-</span></div>
    <div class="metric"><span>Total Throttled</span><span class="val val-denied" id="denied">-</span></div>
  </div>
  <script>
    async function update() {
      try {
        const res = await fetch('/metrics');
        const d = await res.json();
        document.getElementById('tokens').textContent = d.tokens_available.toLocaleString();
        document.getElementById('rate').textContent = d.rate_per_sec.toLocaleString() + ' /s';
        document.getElementById('burst').textContent = d.burst_capacity.toLocaleString();
        document.getElementById('allowed').textContent = d.allowed.toLocaleString();
        document.getElementById('denied').textContent = d.denied.toLocaleString();
      } catch (e) {}
    }
    setInterval(update, 500);
    update();
  </script>
</body>
</html>`
