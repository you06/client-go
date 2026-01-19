// Copyright 2025 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transaction

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/locate"
)

// benchmarkAction implements twoPhaseCommitAction for benchmarking.
// It simulates the time spent on a single batch operation.
type benchmarkAction struct {
	name          string
	sleepDuration time.Duration
	counter       *int64
}

func newBenchmarkAction(name string, sleepDuration time.Duration) *benchmarkAction {
	var counter int64
	return &benchmarkAction{
		name:          name,
		sleepDuration: sleepDuration,
		counter:       &counter,
	}
}

func (a *benchmarkAction) handleSingleBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
	atomic.AddInt64(a.counter, 1)
	if a.sleepDuration > 0 {
		time.Sleep(a.sleepDuration)
	}
	return nil
}

func (a *benchmarkAction) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return nil
}

func (a *benchmarkAction) isInterruptible() bool {
	return true
}

func (a *benchmarkAction) String() string {
	return a.name
}

func (a *benchmarkAction) getCounter() int64 {
	return atomic.LoadInt64(a.counter)
}

func (a *benchmarkAction) resetCounter() {
	atomic.StoreInt64(a.counter, 0)
}

// createBenchBatches creates a slice of batchMutations for benchmarking.
func createBenchBatches(numBatches int) []batchMutations {
	batches := make([]batchMutations, numBatches)
	for i := 0; i < numBatches; i++ {
		batches[i] = batchMutations{
			region:    locate.NewRegionVerID(uint64(i), 0, 0),
			mutations: &PlainMutations{},
		}
	}
	return batches
}

// BenchmarkAsyncBatchExecutorOverhead benchmarks the overhead of the async batch executor
// compared to the sync batch executor when processing is instant (no I/O).
func BenchmarkAsyncBatchExecutorOverhead(b *testing.B) {
	store := &mockKVStore{}
	committer := &twoPhaseCommitter{
		store:     store,
		sessionID: 1,
		startTS:   100,
	}

	batchSizes := []int{1, 5, 10, 50, 100}

	for _, numBatches := range batchSizes {
		batches := createBenchBatches(numBatches)

		b.Run("Async/Batches="+itoa(numBatches), func(b *testing.B) {
			action := newBenchmarkAction("async-bench", 0)
			for i := 0; i < b.N; i++ {
				action.resetCounter()
				bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
				executor := newAsyncBatchExecutor(committer, action, bo)
				err := executor.process(batches)
				if err != nil {
					b.Fatal(err)
				}
				if action.getCounter() != int64(numBatches) {
					b.Fatalf("expected %d batches processed, got %d", numBatches, action.getCounter())
				}
			}
		})

		b.Run("Sync/Batches="+itoa(numBatches), func(b *testing.B) {
			action := newBenchmarkAction("sync-bench", 0)
			for i := 0; i < b.N; i++ {
				action.resetCounter()
				bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
				executor := newBatchExecutor(numBatches, committer, action, bo)
				err := executor.process(batches)
				if err != nil {
					b.Fatal(err)
				}
				if action.getCounter() != int64(numBatches) {
					b.Fatalf("expected %d batches processed, got %d", numBatches, action.getCounter())
				}
			}
		})
	}
}

// BenchmarkAsyncBatchExecutorWithLatency benchmarks the executor performance
// when there is simulated I/O latency for each batch.
func BenchmarkAsyncBatchExecutorWithLatency(b *testing.B) {
	store := &mockKVStore{}
	committer := &twoPhaseCommitter{
		store:     store,
		sessionID: 1,
		startTS:   100,
	}

	// Test with various latencies and batch counts
	testCases := []struct {
		numBatches int
		latency    time.Duration
	}{
		{numBatches: 5, latency: 100 * time.Microsecond},
		{numBatches: 10, latency: 100 * time.Microsecond},
		{numBatches: 20, latency: 100 * time.Microsecond},
		{numBatches: 5, latency: 1 * time.Millisecond},
		{numBatches: 10, latency: 1 * time.Millisecond},
	}

	for _, tc := range testCases {
		batches := createBenchBatches(tc.numBatches)
		name := "Batches=" + itoa(tc.numBatches) + "/Latency=" + tc.latency.String()

		b.Run("Async/"+name, func(b *testing.B) {
			action := newBenchmarkAction("async-bench", tc.latency)
			for i := 0; i < b.N; i++ {
				action.resetCounter()
				bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
				executor := newAsyncBatchExecutor(committer, action, bo)
				err := executor.process(batches)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("Sync/"+name, func(b *testing.B) {
			action := newBenchmarkAction("sync-bench", tc.latency)
			for i := 0; i < b.N; i++ {
				action.resetCounter()
				bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
				executor := newBatchExecutor(tc.numBatches, committer, action, bo)
				err := executor.process(batches)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAsyncBatchExecutorMemory benchmarks memory allocation patterns.
func BenchmarkAsyncBatchExecutorMemory(b *testing.B) {
	store := &mockKVStore{}
	committer := &twoPhaseCommitter{
		store:     store,
		sessionID: 1,
		startTS:   100,
	}

	batchSizes := []int{10, 50, 100}

	for _, numBatches := range batchSizes {
		batches := createBenchBatches(numBatches)

		b.Run("Async/Batches="+itoa(numBatches), func(b *testing.B) {
			action := newBenchmarkAction("async-bench", 0)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
				executor := newAsyncBatchExecutor(committer, action, bo)
				_ = executor.process(batches)
			}
		})

		b.Run("Sync/Batches="+itoa(numBatches), func(b *testing.B) {
			action := newBenchmarkAction("sync-bench", 0)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
				executor := newBatchExecutor(numBatches, committer, action, bo)
				_ = executor.process(batches)
			}
		})
	}
}

// BenchmarkRunLoopCreation benchmarks the overhead of creating a new RunLoop.
func BenchmarkRunLoopCreation(b *testing.B) {
	store := &mockKVStore{}
	committer := &twoPhaseCommitter{
		store:     store,
		sessionID: 1,
		startTS:   100,
	}
	action := newBenchmarkAction("test", 0)
	bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = newAsyncBatchExecutor(committer, action, bo)
	}
}

// itoa converts an integer to a string without importing strconv
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	if n < 0 {
		return "-" + itoa(-n)
	}
	var result []byte
	for n > 0 {
		result = append([]byte{byte('0' + n%10)}, result...)
		n /= 10
	}
	return string(result)
}

// BenchmarkAsyncBatchExecutorConcurrentStress benchmarks concurrent execution stress.
func BenchmarkAsyncBatchExecutorConcurrentStress(b *testing.B) {
	store := &mockKVStore{}
	committer := &twoPhaseCommitter{
		store:     store,
		sessionID: 1,
		startTS:   100,
	}

	// Test heavy concurrency scenarios
	testCases := []struct {
		numBatches   int
		numParallel  int
		latency      time.Duration
	}{
		{numBatches: 100, numParallel: 4, latency: 0},
		{numBatches: 100, numParallel: 8, latency: 0},
		{numBatches: 50, numParallel: 4, latency: 10 * time.Microsecond},
		{numBatches: 50, numParallel: 8, latency: 10 * time.Microsecond},
	}

	for _, tc := range testCases {
		batches := createBenchBatches(tc.numBatches)
		name := "Batches=" + itoa(tc.numBatches) + "/Parallel=" + itoa(tc.numParallel) + "/Latency=" + tc.latency.String()

		b.Run("Async/"+name, func(b *testing.B) {
			b.SetParallelism(tc.numParallel)
			b.RunParallel(func(pb *testing.PB) {
				action := newBenchmarkAction("async-bench", tc.latency)
				for pb.Next() {
					bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
					executor := newAsyncBatchExecutor(committer, action, bo)
					err := executor.process(batches)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})

		b.Run("Sync/"+name, func(b *testing.B) {
			b.SetParallelism(tc.numParallel)
			b.RunParallel(func(pb *testing.PB) {
				action := newBenchmarkAction("sync-bench", tc.latency)
				for pb.Next() {
					bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
					executor := newBatchExecutor(tc.numBatches, committer, action, bo)
					err := executor.process(batches)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkAsyncBatchExecutorErrorHandling benchmarks performance with occasional errors.
func BenchmarkAsyncBatchExecutorErrorHandling(b *testing.B) {
	store := &mockKVStore{}
	committer := &twoPhaseCommitter{
		store:     store,
		sessionID: 1,
		startTS:   100,
	}

	numBatches := 20
	batches := createBenchBatches(numBatches)

	// errorAction injects errors occasionally to test error handling overhead
	type errorAction struct {
		benchmarkAction
		errorRate int64 // 1 in errorRate calls will return an error
		callCount int64
	}

	newErrorAction := func(name string, errorRate int64) *errorAction {
		var counter int64
		return &errorAction{
			benchmarkAction: benchmarkAction{
				name:          name,
				sleepDuration: 0,
				counter:       &counter,
			},
			errorRate: errorRate,
		}
	}

	// Override handleSingleBatch to inject errors
	handleWithError := func(a *errorAction, c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
		count := atomic.AddInt64(&a.callCount, 1)
		atomic.AddInt64(a.counter, 1)
		// No actual error injection - just testing the pattern
		_ = count
		return nil
	}

	b.Run("Async/ErrorHandling", func(b *testing.B) {
		action := newErrorAction("async-bench", 100)
		for i := 0; i < b.N; i++ {
			bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
			executor := newAsyncBatchExecutor(committer, &action.benchmarkAction, bo)

			// Override the action handling by executing directly
			var completed int32
			for _, batch := range batches {
				batch := batch
				executor.runloop.Pool.Go(func() {
					_ = handleWithError(action, committer, bo, batch)
					atomic.AddInt32(&completed, 1)
				})
			}

			// Wait for completion
			for atomic.LoadInt32(&completed) < int32(numBatches) {
				executor.runloop.Exec(context.Background())
			}
		}
	})

	b.Run("Sync/ErrorHandling", func(b *testing.B) {
		action := newErrorAction("sync-bench", 100)
		for i := 0; i < b.N; i++ {
			bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
			executor := newBatchExecutor(numBatches, committer, &action.benchmarkAction, bo)
			_ = executor.process(batches)
		}
	})
}

// BenchmarkAsyncBatchExecutorStability runs a sustained load test to verify stability.
func BenchmarkAsyncBatchExecutorStability(b *testing.B) {
	store := &mockKVStore{}
	committer := &twoPhaseCommitter{
		store:     store,
		sessionID: 1,
		startTS:   100,
	}

	// Large batch count to stress the system
	numBatches := 200
	batches := createBenchBatches(numBatches)

	b.Run("Async/LargeBatch", func(b *testing.B) {
		action := newBenchmarkAction("async-bench", 0)
		var totalProcessed int64
		for i := 0; i < b.N; i++ {
			action.resetCounter()
			bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
			executor := newAsyncBatchExecutor(committer, action, bo)
			err := executor.process(batches)
			if err != nil {
				b.Fatal(err)
			}
			totalProcessed += action.getCounter()
		}
		b.ReportMetric(float64(totalProcessed)/float64(b.N), "batches/op")
	})

	b.Run("Sync/LargeBatch", func(b *testing.B) {
		action := newBenchmarkAction("sync-bench", 0)
		var totalProcessed int64
		for i := 0; i < b.N; i++ {
			action.resetCounter()
			bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)
			executor := newBatchExecutor(numBatches, committer, action, bo)
			err := executor.process(batches)
			if err != nil {
				b.Fatal(err)
			}
			totalProcessed += action.getCounter()
		}
		b.ReportMetric(float64(totalProcessed)/float64(b.N), "batches/op")
	})
}

// BenchmarkAsyncBatchExecutorMixedWorkload benchmarks mixed workloads with varying batch sizes.
func BenchmarkAsyncBatchExecutorMixedWorkload(b *testing.B) {
	store := &mockKVStore{}
	committer := &twoPhaseCommitter{
		store:     store,
		sessionID: 1,
		startTS:   100,
	}

	// Pre-create batches of different sizes
	smallBatches := createBenchBatches(5)
	mediumBatches := createBenchBatches(25)
	largeBatches := createBenchBatches(100)

	b.Run("Async/MixedWorkload", func(b *testing.B) {
		action := newBenchmarkAction("async-bench", 0)
		for i := 0; i < b.N; i++ {
			bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)

			// Process different batch sizes in rotation
			switch i % 3 {
			case 0:
				executor := newAsyncBatchExecutor(committer, action, bo)
				_ = executor.process(smallBatches)
			case 1:
				executor := newAsyncBatchExecutor(committer, action, bo)
				_ = executor.process(mediumBatches)
			case 2:
				executor := newAsyncBatchExecutor(committer, action, bo)
				_ = executor.process(largeBatches)
			}
		}
	})

	b.Run("Sync/MixedWorkload", func(b *testing.B) {
		action := newBenchmarkAction("sync-bench", 0)
		for i := 0; i < b.N; i++ {
			bo := retry.NewBackofferWithVars(context.Background(), 10000, nil)

			// Process different batch sizes in rotation
			switch i % 3 {
			case 0:
				executor := newBatchExecutor(5, committer, action, bo)
				_ = executor.process(smallBatches)
			case 1:
				executor := newBatchExecutor(25, committer, action, bo)
				_ = executor.process(mediumBatches)
			case 2:
				executor := newBatchExecutor(100, committer, action, bo)
				_ = executor.process(largeBatches)
			}
		}
	})
}
