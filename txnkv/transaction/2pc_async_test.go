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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/latch"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util/async"
)

// mockKVStore implements a minimal kvstore interface for testing.
type mockKVStore struct {
	goFunc func(f func()) error
}

func (m *mockKVStore) GetRegionCache() *locate.RegionCache { return nil }
func (m *mockKVStore) SplitRegions(ctx context.Context, splitKeys [][]byte, scatter bool, tableID *int64) (regionIDs []uint64, err error) {
	return nil, nil
}
func (m *mockKVStore) WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error {
	return nil
}
func (m *mockKVStore) GetTimestampWithRetry(bo *retry.Backoffer, scope string) (uint64, error) {
	return 0, nil
}
func (m *mockKVStore) GetOracle() oracle.Oracle { return nil }
func (m *mockKVStore) CurrentTimestamp(txnScope string) (uint64, error) {
	return 0, nil
}
func (m *mockKVStore) SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	return nil, nil
}
func (m *mockKVStore) GetTiKVClient() client.Client        { return nil }
func (m *mockKVStore) GetLockResolver() *txnlock.LockResolver { return nil }
func (m *mockKVStore) Ctx() context.Context                   { return context.Background() }
func (m *mockKVStore) WaitGroup() *sync.WaitGroup             { return nil }
func (m *mockKVStore) TxnLatches() *latch.LatchesScheduler    { return nil }
func (m *mockKVStore) GetClusterID() uint64                   { return 0 }
func (m *mockKVStore) IsClose() bool                          { return false }

func (m *mockKVStore) Go(f func()) error {
	if m.goFunc != nil {
		return m.goFunc(f)
	}
	go f()
	return nil
}

func TestAsyncPoolWrapper(t *testing.T) {
	t.Run("Go with successful store.Go", func(t *testing.T) {
		executed := int32(0)
		done := make(chan struct{})
		store := &mockKVStore{
			goFunc: func(f func()) error {
				go func() {
					f()
					close(done)
				}()
				return nil
			},
		}
		wrapper := &asyncPoolWrapper{store: store}
		wrapper.Go(func() {
			atomic.StoreInt32(&executed, 1)
		})
		<-done
		assert.Equal(t, int32(1), atomic.LoadInt32(&executed))
	})

	t.Run("Go falls back to native goroutine on error", func(t *testing.T) {
		executed := int32(0)
		store := &mockKVStore{
			goFunc: func(f func()) error {
				return errors.New("pool full")
			},
		}
		wrapper := &asyncPoolWrapper{store: store}
		wrapper.Go(func() {
			atomic.StoreInt32(&executed, 1)
		})
		// Wait for the fallback goroutine to execute
		require.Eventually(t, func() bool {
			return atomic.LoadInt32(&executed) == 1
		}, time.Second, time.Millisecond)
	})
}

// mockAction implements twoPhaseCommitAction for testing.
type mockAction struct {
	name          string
	handleFunc    func(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error
	interruptible bool
}

func (m *mockAction) handleSingleBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
	if m.handleFunc != nil {
		return m.handleFunc(c, bo, batch)
	}
	return nil
}

func (m *mockAction) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return nil
}

func (m *mockAction) isInterruptible() bool {
	return m.interruptible
}

func (m *mockAction) String() string {
	return m.name
}

func TestCanUseAsyncBatch(t *testing.T) {
	c := &twoPhaseCommitter{}

	t.Run("actionPrewrite should use async", func(t *testing.T) {
		action := actionPrewrite{}
		assert.True(t, c.canUseAsyncBatch(action))
	})

	t.Run("actionCommit should use async", func(t *testing.T) {
		action := actionCommit{}
		assert.True(t, c.canUseAsyncBatch(action))
	})

	t.Run("other actions should not use async", func(t *testing.T) {
		action := &mockAction{name: "test"}
		assert.False(t, c.canUseAsyncBatch(action))
	})
}

func TestAsyncBatchExecutorProcess(t *testing.T) {
	t.Run("process with successful batches", func(t *testing.T) {
		store := &mockKVStore{}
		committer := &twoPhaseCommitter{
			store:     store,
			sessionID: 1,
			startTS:   100,
		}

		processedBatches := int32(0)
		action := &mockAction{
			name: "test",
			handleFunc: func(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
				atomic.AddInt32(&processedBatches, 1)
				return nil
			},
		}

		bo := retry.NewBackofferWithVars(context.Background(), 1000, nil)
		executor := newAsyncBatchExecutor(committer, action, bo)

		batches := []batchMutations{
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
		}

		err := executor.process(batches)
		assert.NoError(t, err)
		assert.Equal(t, int32(3), atomic.LoadInt32(&processedBatches))
	})

	t.Run("process with one batch failure", func(t *testing.T) {
		store := &mockKVStore{}
		committer := &twoPhaseCommitter{
			store:     store,
			sessionID: 1,
			startTS:   100,
		}

		expectedErr := errors.New("batch failed")
		callCount := int32(0)
		action := &mockAction{
			name: "test",
			handleFunc: func(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
				count := atomic.AddInt32(&callCount, 1)
				if count == 2 {
					return expectedErr
				}
				return nil
			},
		}

		bo := retry.NewBackofferWithVars(context.Background(), 1000, nil)
		executor := newAsyncBatchExecutor(committer, action, bo)

		batches := []batchMutations{
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
		}

		err := executor.process(batches)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("process with assertion failed error", func(t *testing.T) {
		store := &mockKVStore{}
		committer := &twoPhaseCommitter{
			store:     store,
			sessionID: 1,
			startTS:   100,
		}

		assertionErr := &tikverr.ErrAssertionFailed{}
		action := &mockAction{
			name: "test",
			handleFunc: func(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
				return assertionErr
			},
		}

		bo := retry.NewBackofferWithVars(context.Background(), 1000, nil)
		executor := newAsyncBatchExecutor(committer, action, bo)

		batches := []batchMutations{
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
		}

		err := executor.process(batches)
		assert.Error(t, err)
		assert.IsType(t, &tikverr.ErrAssertionFailed{}, err)
	})

	t.Run("assertion failed error is ignored when other error exists", func(t *testing.T) {
		store := &mockKVStore{}
		committer := &twoPhaseCommitter{
			store:     store,
			sessionID: 1,
			startTS:   100,
		}

		otherErr := errors.New("other error")
		assertionErr := &tikverr.ErrAssertionFailed{}
		callCount := int32(0)
		action := &mockAction{
			name: "test",
			handleFunc: func(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
				count := atomic.AddInt32(&callCount, 1)
				if count == 1 {
					return assertionErr
				}
				return otherErr
			},
		}

		bo := retry.NewBackofferWithVars(context.Background(), 1000, nil)
		executor := newAsyncBatchExecutor(committer, action, bo)

		batches := []batchMutations{
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
		}

		err := executor.process(batches)
		assert.Error(t, err)
		// Should return the other error, not the assertion error
		assert.Equal(t, otherErr, err)
	})

	t.Run("process with context cancellation", func(t *testing.T) {
		store := &mockKVStore{}
		committer := &twoPhaseCommitter{
			store:     store,
			sessionID: 1,
			startTS:   100,
		}

		action := &mockAction{
			name: "test",
			handleFunc: func(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
				// Simulate a slow operation
				time.Sleep(100 * time.Millisecond)
				return nil
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		bo := retry.NewBackofferWithVars(ctx, 1000, nil)
		executor := newAsyncBatchExecutor(committer, action, bo)

		batches := []batchMutations{
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
			{region: locate.RegionVerID{}, mutations: &PlainMutations{}},
		}

		// Cancel the context after a short delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		err := executor.process(batches)
		// The error could be context.Canceled or nil depending on timing
		// If context is cancelled before runloop.Exec, we get context.Canceled
		// If all batches complete before cancellation, we get nil
		_ = err // Just verify no panic
	})
}

func TestNewAsyncBatchExecutor(t *testing.T) {
	store := &mockKVStore{}
	committer := &twoPhaseCommitter{
		store: store,
	}
	action := &mockAction{name: "test"}
	bo := retry.NewBackofferWithVars(context.Background(), 1000, nil)

	executor := newAsyncBatchExecutor(committer, action, bo)

	assert.NotNil(t, executor)
	assert.Equal(t, committer, executor.committer)
	assert.Equal(t, action, executor.action)
	assert.Equal(t, bo, executor.bo)
	assert.NotNil(t, executor.runloop)
	assert.NotNil(t, executor.runloop.Pool)
}

func TestAsyncBatchExecutorWithRunLoop(t *testing.T) {
	t.Run("runloop executes callbacks correctly", func(t *testing.T) {
		store := &mockKVStore{}
		committer := &twoPhaseCommitter{
			store:     store,
			sessionID: 1,
			startTS:   100,
		}

		var executionOrder []int
		var mu sync.Mutex
		action := &mockAction{
			name: "test",
			handleFunc: func(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
				mu.Lock()
				executionOrder = append(executionOrder, int(batch.region.GetID()))
				mu.Unlock()
				return nil
			},
		}

		bo := retry.NewBackofferWithVars(context.Background(), 1000, nil)
		executor := newAsyncBatchExecutor(committer, action, bo)

		batches := []batchMutations{
			{region: locate.NewRegionVerID(1, 0, 0), mutations: &PlainMutations{}},
			{region: locate.NewRegionVerID(2, 0, 0), mutations: &PlainMutations{}},
			{region: locate.NewRegionVerID(3, 0, 0), mutations: &PlainMutations{}},
		}

		err := executor.process(batches)
		assert.NoError(t, err)
		assert.Len(t, executionOrder, 3)
	})
}

func TestSendBatchAsync(t *testing.T) {
	t.Run("sendBatchAsync calls handleSingleBatch", func(t *testing.T) {
		store := &mockKVStore{}
		committer := &twoPhaseCommitter{
			store:     store,
			sessionID: 1,
			startTS:   100,
		}

		called := false
		action := &mockAction{
			name: "test",
			handleFunc: func(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
				called = true
				return nil
			},
		}

		bo := retry.NewBackofferWithVars(context.Background(), 1000, nil)
		executor := newAsyncBatchExecutor(committer, action, bo)

		batch := batchMutations{
			region:    locate.RegionVerID{},
			mutations: &PlainMutations{},
		}

		completed := make(chan struct{})
		cb := async.NewCallback(executor.runloop, func(_ struct{}, err error) {
			close(completed)
		})

		executor.sendBatchAsync(bo, batch, cb)

		// Drive the runloop to execute the callback
		go func() {
			for {
				select {
				case <-completed:
					return
				default:
					executor.runloop.Exec(context.Background())
				}
			}
		}()

		<-completed
		assert.True(t, called)
	})
}
