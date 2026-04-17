package bench

import (
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	DirectDSN   string // DSN to connect directly to MySQL (e.g., "user:pass@tcp(host:3306)/db")
	ProxyDSN    string // DSN to connect via proxy (e.g., "user:pass@tcp(host:3307)/db")
	Queries     []string
	Concurrency int
	Iterations  int
	WarmupIters int
}

type Result struct {
	Label    string
	Query    string
	Timings  []time.Duration
	Errors   int
}

type BenchmarkReport struct {
	DirectResults []Result
	ProxyResults  []Result
}

func Run(cfg Config) (*BenchmarkReport, error) {
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.Iterations == 0 {
		cfg.Iterations = 100
	}
	if cfg.WarmupIters == 0 {
		cfg.WarmupIters = 10
	}

	directDB, err := sql.Open("mysql", cfg.DirectDSN)
	if err != nil {
		return nil, fmt.Errorf("connecting to direct MySQL: %w", err)
	}
	defer directDB.Close()
	directDB.SetMaxOpenConns(cfg.Concurrency)

	if err := directDB.Ping(); err != nil {
		return nil, fmt.Errorf("pinging direct MySQL: %w", err)
	}

	proxyDB, err := sql.Open("mysql", cfg.ProxyDSN)
	if err != nil {
		return nil, fmt.Errorf("connecting to proxy: %w", err)
	}
	defer proxyDB.Close()
	proxyDB.SetMaxOpenConns(cfg.Concurrency)

	if err := proxyDB.Ping(); err != nil {
		return nil, fmt.Errorf("pinging proxy: %w", err)
	}

	report := &BenchmarkReport{}

	for _, query := range cfg.Queries {
		slog.Info("benchmarking query", "query", truncateQuery(query, 80))

		slog.Info("warmup", "iterations", cfg.WarmupIters)
		runQuery(directDB, query, cfg.WarmupIters, 1)
		runQuery(proxyDB, query, cfg.WarmupIters, 1)

		slog.Info("bench direct", "iterations", cfg.Iterations, "concurrency", cfg.Concurrency)
		directResult := runQuery(directDB, query, cfg.Iterations, cfg.Concurrency)
		directResult.Label = "direct"
		directResult.Query = query

		slog.Info("bench proxy", "iterations", cfg.Iterations, "concurrency", cfg.Concurrency)
		proxyResult := runQuery(proxyDB, query, cfg.Iterations, cfg.Concurrency)
		proxyResult.Label = "proxy"
		proxyResult.Query = query

		report.DirectResults = append(report.DirectResults, directResult)
		report.ProxyResults = append(report.ProxyResults, proxyResult)
	}

	return report, nil
}

func runQuery(db *sql.DB, query string, iterations, concurrency int) Result {
	var mu sync.Mutex
	result := Result{
		Timings: make([]time.Duration, 0, iterations),
	}

	var wg sync.WaitGroup
	itersPerWorker := iterations / concurrency
	remainder := iterations % concurrency

	for w := 0; w < concurrency; w++ {
		count := itersPerWorker
		if w < remainder {
			count++
		}

		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				start := time.Now()
				rows, err := db.Query(query)
				if err != nil {
					mu.Lock()
					result.Errors++
					mu.Unlock()
					continue
				}
				for rows.Next() {
					// drain result set
				}
				rows.Close()
				duration := time.Since(start)

				mu.Lock()
				result.Timings = append(result.Timings, duration)
				mu.Unlock()
			}
		}(count)
	}

	wg.Wait()
	return result
}

func (r *BenchmarkReport) Print() {
	fmt.Println()
	fmt.Println("=== Benchmark Results ===")
	fmt.Println()

	for i := range r.DirectResults {
		direct := r.DirectResults[i]
		proxy := r.ProxyResults[i]

		fmt.Printf("Query: %s\n", truncateQuery(direct.Query, 80))
		fmt.Println("---")
		printStats("Direct", direct)
		printStats("Proxy ", proxy)

		if len(direct.Timings) > 0 && len(proxy.Timings) > 0 {
			directP50 := percentile(direct.Timings, 50)
			proxyP50 := percentile(proxy.Timings, 50)
			overhead := float64(proxyP50-directP50) / float64(directP50) * 100
			fmt.Printf("Overhead (p50): %+.1f%%\n", overhead)

			directP99 := percentile(direct.Timings, 99)
			proxyP99 := percentile(proxy.Timings, 99)
			overheadP99 := float64(proxyP99-directP99) / float64(directP99) * 100
			fmt.Printf("Overhead (p99): %+.1f%%\n", overheadP99)
		}
		fmt.Println()
	}
}

func printStats(label string, r Result) {
	if len(r.Timings) == 0 {
		fmt.Printf("  %s: no successful queries (errors: %d)\n", label, r.Errors)
		return
	}

	sort.Slice(r.Timings, func(i, j int) bool { return r.Timings[i] < r.Timings[j] })

	avg := mean(r.Timings)
	p50 := percentile(r.Timings, 50)
	p95 := percentile(r.Timings, 95)
	p99 := percentile(r.Timings, 99)
	min := r.Timings[0]
	max := r.Timings[len(r.Timings)-1]
	sd := stddev(r.Timings, avg)

	fmt.Printf("  %s: avg=%v  p50=%v  p95=%v  p99=%v  min=%v  max=%v  stddev=%v  errors=%d\n",
		label, avg.Round(time.Microsecond), p50.Round(time.Microsecond),
		p95.Round(time.Microsecond), p99.Round(time.Microsecond),
		min.Round(time.Microsecond), max.Round(time.Microsecond),
		sd.Round(time.Microsecond), r.Errors)
}

func mean(timings []time.Duration) time.Duration {
	var total time.Duration
	for _, t := range timings {
		total += t
	}
	return total / time.Duration(len(timings))
}

func percentile(timings []time.Duration, p float64) time.Duration {
	if len(timings) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(timings)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(timings) {
		idx = len(timings) - 1
	}
	return timings[idx]
}

func stddev(timings []time.Duration, avg time.Duration) time.Duration {
	if len(timings) < 2 {
		return 0
	}
	var sumSquares float64
	for _, t := range timings {
		diff := float64(t - avg)
		sumSquares += diff * diff
	}
	return time.Duration(math.Sqrt(sumSquares / float64(len(timings)-1)))
}

func truncateQuery(q string, maxLen int) string {
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen-3] + "..."
}
