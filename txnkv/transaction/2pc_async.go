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
	"math"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
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
func (e *asyncBatchExecutor) sendBatchAsync(
	bo *retry.Backoffer,
	batch batchMutations,
	cb async.Callback[struct{}],
) {
	// Dispatch based on action type.
	switch act := e.action.(type) {
	case actionPrewrite:
		e.sendPrewriteAsync(bo, batch, act, cb)
	default:
		// Fall back to sync mode for unsupported actions.
		cb.Executor().Go(func() {
			err := e.action.handleSingleBatch(e.committer, bo, batch)
			cb.Schedule(struct{}{}, err)
		})
	}
}

// sendPrewriteAsync asynchronously sends a prewrite request for a single batch.
func (e *asyncBatchExecutor) sendPrewriteAsync(
	bo *retry.Backoffer,
	batch batchMutations,
	action actionPrewrite,
	cb async.Callback[struct{}],
) {
	c := e.committer

	// Handle failpoint for testing.
	if err := action.handleSingleBatchFailpoint(c, bo, batch); err != nil {
		cb.Invoke(struct{}{}, err)
		return
	}

	// Build the prewrite request.
	txnSize := uint64(c.regionTxnSize[batch.region.GetID()])
	if action.retry {
		txnSize = math.MaxUint64
	}
	req := c.buildPrewriteRequest(batch, txnSize)

	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient(), c.store.GetOracle())
	reqBegin := time.Now()

	onResp := func(resp *tikvrpc.ResponseExt, err error) {
		// Handle RPC error.
		if err != nil {
			e.handlePrewriteRPCError(c, sender, cb, err)
			return
		}

		// Check for region error.
		regionErr, err := resp.GetRegionError()
		if err != nil {
			cb.Invoke(struct{}{}, err)
			return
		}

		if regionErr != nil {
			// Region error: fall back to sync mode for retry.
			e.handlePrewriteRegionError(bo, batch, action, cb)
			return
		}

		// Check for missing response body.
		if resp.Response.Resp == nil {
			cb.Invoke(struct{}{}, errors.WithStack(tikverr.ErrBodyMissing))
			return
		}

		prewriteResp := resp.Response.Resp.(*kvrpcpb.PrewriteResponse)
		keyErrs := prewriteResp.GetErrors()

		if len(keyErrs) == 0 {
			// Success: handle the successful response.
			err := e.handlePrewriteSuccess(c, sender, batch, reqBegin, prewriteResp, bo)
			cb.Invoke(struct{}{}, err)
			return
		}

		// Key errors: fall back to sync mode for lock resolution.
		e.handlePrewriteKeyErrors(bo, batch, action, cb)
	}

	sender.SendReqAsync(bo, req, batch.region, client.ReadTimeoutShort, async.NewCallback(cb.Executor(), onResp))
}

// handlePrewriteRPCError handles RPC errors during async prewrite.
func (e *asyncBatchExecutor) handlePrewriteRPCError(
	c *twoPhaseCommitter,
	sender *locate.RegionRequestSender,
	cb async.Callback[struct{}],
	rpcErr error,
) {
	// For async commit and 1PC, RPC error means undetermined state.
	if (c.isAsyncCommit() || c.isOnePC()) && sender.GetRPCError() != nil && atomic.LoadUint32(&c.prewriteCancelled) == 0 {
		c.setUndeterminedErr(sender.GetRPCError())
	}
	cb.Invoke(struct{}{}, rpcErr)
}

// handlePrewriteRegionError handles region errors by falling back to sync mode.
func (e *asyncBatchExecutor) handlePrewriteRegionError(
	bo *retry.Backoffer,
	batch batchMutations,
	action actionPrewrite,
	cb async.Callback[struct{}],
) {
	cb.Executor().Go(func() {
		// Use the sync path to handle region error and retry.
		err := action.handleSingleBatch(e.committer, bo, batch)
		cb.Schedule(struct{}{}, err)
	})
}

// handlePrewriteKeyErrors handles key errors by falling back to sync mode.
func (e *asyncBatchExecutor) handlePrewriteKeyErrors(
	bo *retry.Backoffer,
	batch batchMutations,
	action actionPrewrite,
	cb async.Callback[struct{}],
) {
	cb.Executor().Go(func() {
		// Use the sync path to handle key errors (lock resolution).
		err := action.handleSingleBatch(e.committer, bo, batch)
		cb.Schedule(struct{}{}, err)
	})
}

// handlePrewriteSuccess handles a successful prewrite response.
func (e *asyncBatchExecutor) handlePrewriteSuccess(
	c *twoPhaseCommitter,
	sender *locate.RegionRequestSender,
	batch batchMutations,
	reqBegin time.Time,
	prewriteResp *kvrpcpb.PrewriteResponse,
	bo *retry.Backoffer,
) error {
	// Clear the RPC Error since the request is evaluated successfully.
	sender.SetRPCError(nil)

	// Update CommitDetails.
	reqDuration := time.Since(reqBegin)
	c.getDetail().MergePrewriteReqDetails(
		reqDuration,
		batch.region.GetID(),
		sender.GetStoreAddr(),
		prewriteResp.ExecDetailsV2,
	)

	// Handle primary key specific logic.
	if batch.isPrimary {
		// After writing the primary key, if the size of the transaction is larger than 32M,
		// start the ttlManager.
		if int64(c.txnSize) > config.GetGlobalConfig().TiKVClient.TTLRefreshedTxnSize &&
			prewriteResp.OnePcCommitTs == 0 {
			c.ttlManager.run(c, nil, false)
		}
	}

	// Handle 1PC response.
	if c.isOnePC() {
		if prewriteResp.OnePcCommitTs == 0 {
			if prewriteResp.MinCommitTs != 0 {
				return errors.New("MinCommitTs must be 0 when 1pc falls back to 2pc")
			}
			logutil.Logger(bo.GetCtx()).Warn(
				"1pc failed and fallbacks to normal commit procedure",
				zap.Uint64("startTS", c.startTS),
			)
			metrics.OnePCTxnCounterFallback.Inc()
			c.setOnePC(false)
			c.setAsyncCommit(false)
		} else {
			if c.onePCCommitTS != 0 {
				logutil.Logger(bo.GetCtx()).Fatal(
					"one pc happened multiple times",
					zap.Uint64("startTS", c.startTS),
				)
			}
			c.onePCCommitTS = prewriteResp.OnePcCommitTs
		}
		return nil
	} else if prewriteResp.OnePcCommitTs != 0 {
		logutil.Logger(bo.GetCtx()).Fatal(
			"tikv committed a non-1pc transaction with 1pc protocol",
			zap.Uint64("startTS", c.startTS),
		)
	}

	// Handle async commit response.
	if c.isAsyncCommit() {
		if prewriteResp.MinCommitTs == 0 {
			if c.testingKnobs.noFallBack {
				return nil
			}
			logutil.Logger(bo.GetCtx()).Warn(
				"async commit cannot proceed since the returned minCommitTS is zero, "+
					"fallback to normal path", zap.Uint64("startTS", c.startTS),
			)
			c.setAsyncCommit(false)
		} else {
			c.mu.Lock()
			if prewriteResp.MinCommitTs > c.minCommitTSMgr.get() {
				c.minCommitTSMgr.tryUpdate(prewriteResp.MinCommitTs, twoPCAccess)
			}
			c.mu.Unlock()
		}
	}

	return nil
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
