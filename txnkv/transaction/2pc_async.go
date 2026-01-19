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

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/util/async"
	"go.uber.org/zap"
)

// asyncPoolWrapper wraps kvstore to implement async.Pool interface.
type asyncPoolWrapper struct {
	store kvstore
}

func (p *asyncPoolWrapper) Go(f func()) {
	err := p.store.Go(f)
	if err != nil {
		// fallback to native go
		go f()
	}
}

// asyncBatchExecutor uses async RPC to execute batch operations.
type asyncBatchExecutor struct {
	committer *twoPhaseCommitter
	action    twoPhaseCommitAction
	bo        *retry.Backoffer
	runloop   *async.RunLoop
}

// newAsyncBatchExecutor creates a new asyncBatchExecutor.
func newAsyncBatchExecutor(
	committer *twoPhaseCommitter,
	action twoPhaseCommitAction,
	bo *retry.Backoffer,
) *asyncBatchExecutor {
	runloop := async.NewRunLoop()
	runloop.Pool = &asyncPoolWrapper{store: committer.store}
	return &asyncBatchExecutor{
		committer: committer,
		action:    action,
		bo:        bo,
		runloop:   runloop,
	}
}

// process asynchronously processes all batches.
func (e *asyncBatchExecutor) process(batches []batchMutations) error {
	var (
		completed          int32
		err                error
		assertionFailedErr error
		cancel             context.CancelFunc
	)

	// For prewrite, stop sending other requests after receiving first error.
	// Similar to batchExecutor.process in 2pc.go.
	switch e.action.(type) {
	case actionPrewrite:
		e.bo, cancel = e.bo.Fork()
		defer cancel()
	}

	forkedBo, forkedCancel := e.bo.Fork()
	defer forkedCancel()

	// Send all requests asynchronously.
	for i, batch1 := range batches {
		var backoffer *retry.Backoffer
		if i == len(batches)-1 {
			backoffer = forkedBo
		} else {
			backoffer = forkedBo.Clone()
		}

		batch := batch1
		e.sendBatchAsync(backoffer, batch, async.NewCallback(e.runloop, func(_ struct{}, batchErr error) {
			// The callback is executed in the runloop's goroutine, so it's safe to update
			// variables without locks.
			atomic.AddInt32(&completed, 1)
			if batchErr != nil {
				logutil.BgLogger().Debug("2PC doActionOnBatch async failed",
					zap.Uint64("session", e.committer.sessionID),
					zap.Stringer("action type", e.action),
					zap.Error(batchErr),
					zap.Uint64("txnStartTS", e.committer.startTS))

				if _, isAssertionFailed := errors.Cause(batchErr).(*tikverr.ErrAssertionFailed); isAssertionFailed {
					if assertionFailedErr == nil {
						assertionFailedErr = batchErr
					}
				} else {
					// Cancel other requests and record the first error.
					if cancel != nil {
						logutil.BgLogger().Debug("2PC doActionOnBatch async to cancel other actions",
							zap.Uint64("session", e.committer.sessionID),
							zap.Stringer("action type", e.action),
							zap.Uint64("txnStartTS", e.committer.startTS))
						atomic.StoreUint32(&e.committer.prewriteCancelled, 1)
						cancel()
					}
					if err == nil {
						err = batchErr
					}
				}
			}
		}))
	}

	// Drive the runloop until all requests are completed.
	for atomic.LoadInt32(&completed) < int32(len(batches)) {
		if _, execErr := e.runloop.Exec(e.bo.GetCtx()); execErr != nil {
			if err == nil {
				err = errors.WithStack(execErr)
			}
			break
		}
	}

	if err != nil {
		if assertionFailedErr != nil {
			logutil.BgLogger().Debug("2PC doActionOnBatch async met assertion failed error but ignored due to other kinds of error",
				zap.Uint64("session", e.committer.sessionID),
				zap.Stringer("action type", e.action),
				zap.Uint64("txnStartTS", e.committer.startTS),
				zap.Uint64("forUpdateTS", e.committer.forUpdateTS),
				zap.NamedError("assertionFailed", assertionFailedErr),
				zap.Error(err))
		}
		return err
	}
	return assertionFailedErr
}

// sendBatchAsync asynchronously sends a single batch request.
// In Phase 1, all requests fall back to sync mode by calling handleSingleBatch.
func (e *asyncBatchExecutor) sendBatchAsync(
	bo *retry.Backoffer,
	batch batchMutations,
	cb async.Callback[struct{}],
) {
	// Phase 1: Fall back to sync mode for all actions.
	// In subsequent phases, we will implement async handling for prewrite and commit.
	cb.Executor().Go(func() {
		err := e.action.handleSingleBatch(e.committer, bo, batch)
		cb.Schedule(struct{}{}, err)
	})
}

// canUseAsyncBatch checks if async batch processing can be used for the given action.
func (c *twoPhaseCommitter) canUseAsyncBatch(action twoPhaseCommitAction) bool {
	switch action.(type) {
	case actionPrewrite, actionCommit:
		// Only enable async mode for prewrite and commit.
		return true
	default:
		return false
	}
}
