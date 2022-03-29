package transaction

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"go.uber.org/zap"
)

type actionWriteIntent struct {
	*kv.LockCtx
}

// type actionWriteIntentRollback struct{}

func (action actionWriteIntent) buildWriteIntentRequest(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) (*tikvrpc.Request, error) {
	memBuf := c.txn.GetMemBuffer()
	filter := c.txn.kvFilter
	m := batch.mutations
	mutations := make([]*kvrpcpb.Mutation, 0, m.Len())
	isPessimisticLock := make([]bool, 0, m.Len())
	// TODO: get the delta content by memdb directly.
	for i := 0; i < m.Len(); i++ {
		key := m.GetKey(i)
		value, flags, err := memBuf.GetWithFlag(key)
		if err != nil {
			return nil, err
		}
		if isShortValue(value) {
			continue
		}
		//flagOnly := tikverr.IsErrNotFound(err) && val == nil
		flagOnly := value == nil && err == nil
		// skip the flag-only keys, such keys should be locked in prewrite phase.
		if flagOnly {
			continue
		}
		var (
			op              kvrpcpb.Op
			isUnnecessaryKV bool
		)
		if filter != nil {
			isUnnecessaryKV, err = filter.IsUnnecessaryKeyValue(key, value, flags)
			if err != nil {
				return nil, err
			}
		}
		if isUnnecessaryKV {
			continue
		}
		if len(value) > 0 {
			op = kvrpcpb.Op_Put
			if flags.HasPresumeKeyNotExists() {
				op = kvrpcpb.Op_Insert
			}
		} else {
			if c.txn.IsPessimistic() || !flags.HasPresumeKeyNotExists() {
				if flags.HasNewlyInserted() {
					// The delete-your-write keys in pessimistic transactions, only lock needed keys and skip
					// other deletes for example the secondary index delete.
					// Here if `tidb_constraint_check_in_place` is enabled and the transaction is in optimistic mode,
					// the logic is same as the pessimistic mode.
					if flags.HasLocked() {
						// this lock is used to delete the possible insert intent
						op = kvrpcpb.Op_Lock
					} else {
						continue
					}
				} else {
					op = kvrpcpb.Op_Del
				}
			} else {
				continue
			}
		}
		mut := &kvrpcpb.Mutation{
			Op:    op,
			Key:   m.GetKey(i),
			Value: value,
		}
		// TODO: add assertion
		//if c.txn.us.HasPresumeKeyNotExists(m.GetKey(i)) || (c.doingAmend && m.GetOp(i) == kvrpcpb.Op_Insert) {
		//	mut.Assertion = kvrpcpb.Assertion_NotExist
		//}
		mutations = append(mutations, mut)
		isPessimisticLock = append(isPessimisticLock, true)
	}

	req := &kvrpcpb.PrewriteRequest{
		Mutations:         mutations,
		PrimaryLock:       c.primary(),
		StartVersion:      c.startTS,
		LockTtl:           0,
		IsPessimisticLock: isPessimisticLock,
		ForUpdateTs:       c.forUpdateTS,
		TxnSize:           0,
		MinCommitTs:       0,
		MaxCommitTs:       c.maxCommitTS,
		AssertionLevel:    kvrpcpb.AssertionLevel_Off,
		// mark it a write-intent request
		WriteIntent: kvrpcpb.Intent_WriteIntent,
	}

	return tikvrpc.NewRequest(tikvrpc.CmdPrewrite, req, kvrpcpb.Context{Priority: c.priority, SyncLog: c.syncLog, ResourceGroupTag: action.LockCtx.ResourceGroupTag,
		MaxExecutionDurationMs: uint64(client.MaxWriteExecutionTime.Milliseconds())}), nil
}

func (action actionWriteIntent) handleSingleBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
	// no need to write intent
	if batch.mutations.Len() == 0 || !c.txn.isPessimistic {
		return nil
	}
	req, err := action.buildWriteIntentRequest(c, bo, batch)
	if err != nil {
		return err
	}
	// get write indent result
	c.onFlyIntents.Lock()
	c.onFlyIntents.intents[c.forUpdateTS] += 1
	c.onFlyIntents.Unlock()
	// write intents asynchronously
	go func() {
		tBegin := time.Now()
		attempts := 0
		sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
		for {
			attempts++
			if time.Since(tBegin) > slowRequestThreshold {
				logutil.BgLogger().Warn("slow write-intent request", zap.Uint64("startTS", c.startTS), zap.Stringer("region", &batch.region), zap.Int("attempts", attempts))
				tBegin = time.Now()
			}

			resp, err := sender.SendReq(bo, req, batch.region, client.ReadTimeoutShort)
			// Unexpected error occurs, return it
			if err != nil {
				logutil.Logger(bo.GetCtx()).Error("write-intent request, send fail", zap.Error(err))
				return
			}

			regionErr, err := resp.GetRegionError()
			if err != nil {
				logutil.Logger(bo.GetCtx()).Error("write-intent request, get region fail", zap.Error(err))
				return
			}
			if regionErr != nil {
				// For other region error and the fake region error, backoff because
				// there's something wrong.
				// For the real EpochNotMatch error, don't backoff.
				if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
					err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
					if err != nil {
						logutil.Logger(bo.GetCtx()).Error("write-intent request, backoff fail", zap.Error(err))
						return
					}
				}
				if regionErr.GetDiskFull() != nil {
					storeIds := regionErr.GetDiskFull().GetStoreId()
					desc := " "
					for _, i := range storeIds {
						desc += strconv.FormatUint(i, 10) + " "
					}

					logutil.Logger(bo.GetCtx()).Error("Request failed cause of TiKV disk full",
						zap.String("store_id", desc),
						zap.String("reason", regionErr.GetDiskFull().GetReason()))
					return
				}
				same, err := batch.relocate(bo, c.store.GetRegionCache())
				if err != nil {
					logutil.Logger(bo.GetCtx()).Error("write-intent request, relocate fail", zap.Error(err))
					return
				}
				if same {
					continue
				}
				err = c.doActionOnMutations(bo, actionWriteIntent{action.LockCtx}, batch.mutations)
				if err != nil {
					logutil.Logger(bo.GetCtx()).Error("write-intent request, retry fail", zap.Error(err))
				}
				break
			}

			if resp.Resp == nil {
				logutil.Logger(bo.GetCtx()).Error("write-intent request, empty resp")
				return
			}
			writeIntentResp := resp.Resp.(*kvrpcpb.PrewriteResponse)
			keyErrs := writeIntentResp.GetErrors()
			if len(keyErrs) == 0 {
				logutil.Logger(bo.GetCtx()).Info("write-intent request success")
				break
			}
			var locks []*txnlock.Lock
			for _, keyErr := range keyErrs {
				// Check already exists error
				if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
					e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
					// ignore the key-exist err by now, handle it in 2PC(fallback)
					err := c.extractKeyExistsErr(e)
					logutil.Logger(bo.GetCtx()).Error("write-intent request, key err", zap.Error(err))
					return
				}

				// Extract lock from key error
				lock, err1 := txnlock.ExtractLockFromKeyErr(keyErr)
				if err1 != nil {
					logutil.Logger(bo.GetCtx()).Error("write-intent request, lock err", zap.Error(err1))
					return
				}
				logutil.BgLogger().Info("prewrite encounters lock",
					zap.Uint64("session", c.sessionID),
					zap.Uint64("txnID", c.startTS),
					zap.Stringer("lock", lock))
				// If an optimistic transaction encounters a lock with larger TS, this transaction will certainly
				// fail due to a WriteConflict error. So we can construct and return an error here early.
				// Pessimistic transactions don't need such an optimization. If this key needs a pessimistic lock,
				// TiKV will return a PessimisticLockNotFound error directly if it encounters a different lock. Otherwise,
				// TiKV returns lock.TTL = 0, and we still need to resolve the lock.
				if lock.TxnID > c.startTS && !c.isPessimistic {
					err := tikverr.NewErrWriteConfictWithArgs(c.startTS, lock.TxnID, 0, lock.Key)
					logutil.Logger(bo.GetCtx()).Error("write-intent request, write-conflict err", zap.Error(err))
				}
				locks = append(locks, lock)
			}
			start := time.Now()
			msBeforeExpired, err := c.store.GetLockResolver().ResolveLocks(bo, c.startTS, locks)
			if err != nil {
				logutil.Logger(bo.GetCtx()).Error("write-intent request, resolve lock err", zap.Error(err))
				return
			}
			atomic.AddInt64(&c.getDetail().ResolveLockTime, int64(time.Since(start)))
			if msBeforeExpired > 0 {
				err = bo.BackoffWithCfgAndMaxSleep(retry.BoTxnLock, int(msBeforeExpired), errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
				if err != nil {
					logutil.Logger(bo.GetCtx()).Error("write-intent request, resolve lock backoff err", zap.Error(err))
					return
				}
			}
		}
		// handle success
		c.onFlyIntents.Lock()
		c.onFlyIntents.intents[c.forUpdateTS] -= 1
		finish := c.onFlyIntents.intents[c.forUpdateTS] == 0
		c.onFlyIntents.Unlock()
		if finish {
			c.onFlyIntents.megChan <- struct{}{}
		}
	}()
	return nil
}

func (actionWriteIntent) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return nil
}

func (actionWriteIntent) String() string {
	return "write_intent"
}
