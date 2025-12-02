// Copyright 2021 TiKV Authors
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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/safepoint_test.go
//

// Copyright 2017 PingCAP, Inc.
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

package tikv_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

func TestSafepoint(t *testing.T) {
	suite.Run(t, new(testSafePointSuite))
}

type testSafePointSuite struct {
	suite.Suite
	store  tikv.StoreProbe
	prefix string
}

func (s *testSafePointSuite) SetupSuite() {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(s.T())}
	s.prefix = fmt.Sprintf("seek_%d", time.Now().Unix())
}

func (s *testSafePointSuite) TearDownSuite() {
	err := s.store.Close()
	s.Require().Nil(err)
}

func (s *testSafePointSuite) beginTxn() transaction.TxnProbe {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	return txn
}

func mymakeKeys(rowNum int, prefix string) [][]byte {
	keys := make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		k := encodeKey(prefix, s08d("key", i))
		keys = append(keys, k)
	}
	return keys
}

func (s *testSafePointSuite) waitUntilErrorPlugIn(t uint64) {
	for {
		s.store.SaveSafePointToSafePointKV(t + 10)
		cachedTime := time.Now()
		newSafePoint, err := s.store.LoadSafePointFromSafePointKV()
		if err == nil {
			s.store.UpdateTxnSafePointCache(newSafePoint, cachedTime)
			break
		}
		time.Sleep(time.Second)
	}
}

func (s *testSafePointSuite) TestSafePoint() {
	txn := s.beginTxn()
	for i := 0; i < 10; i++ {
		err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
		s.Nil(err)
	}
	err := txn.Commit(context.Background())
	s.Nil(err)

	// for txn get
	txn2 := s.beginTxn()
	_, err = txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	s.Nil(err)

	s.waitUntilErrorPlugIn(txn2.StartTS())

	// clean cache for sending request to store.
	txn2.GetSnapshot().CleanCache([][]byte{encodeKey(s.prefix, s08d("key", 0))})
	_, geterr2 := txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	s.NotNil(geterr2)

	_, isFallBehind := errors.Cause(geterr2).(*tikverr.ErrTxnAbortedByGC)
	isMayFallBehind := strings.Contains(geterr2.Error(), "start timestamp may fall behind safe point")
	isBehind := isFallBehind || isMayFallBehind
	s.True(isBehind)

	// for txn seek
	txn3 := s.beginTxn()

	s.waitUntilErrorPlugIn(txn3.StartTS())

	_, seekerr := txn3.Iter(encodeKey(s.prefix, ""), nil)
	s.NotNil(seekerr)
	_, isFallBehind = errors.Cause(geterr2).(*tikverr.ErrTxnAbortedByGC)
	isMayFallBehind = strings.Contains(geterr2.Error(), "start timestamp may fall behind safe point")
	isBehind = isFallBehind || isMayFallBehind
	s.True(isBehind)

	// for snapshot batchGet
	keys := mymakeKeys(10, s.prefix)
	txn4 := s.beginTxn()

	s.waitUntilErrorPlugIn(txn4.StartTS())

	_, batchgeterr := toTiDBTxn(&txn4).BatchGet(context.Background(), toTiDBKeys(keys))
	s.NotNil(batchgeterr)
	_, isFallBehind = errors.Cause(geterr2).(*tikverr.ErrTxnAbortedByGC)
	isMayFallBehind = strings.Contains(geterr2.Error(), "start timestamp may fall behind safe point")
	isBehind = isFallBehind || isMayFallBehind
	s.True(isBehind)
	// sleep to wait for the next transaction will get a valid startTS to make the next test stable.
	time.Sleep(time.Second)
}

func (s *testSafePointSuite) TestGCResolvePessimisticLockDuringCommit() {
	ctx := context.Background()
	primaryKey := encodeKey(s.prefix, "gc_primary")
	secondaryKey := encodeKey(s.prefix, "gc_secondary")

	txn := s.beginTxn()
	txn.SetPessimistic(true)
	lockCtx := kv.NewLockCtx(txn.StartTS(), kv.LockAlwaysWait, time.Now())
	s.Require().NoError(txn.LockKeys(ctx, lockCtx, primaryKey, secondaryKey))
	s.Require().NoError(txn.Set(primaryKey, []byte("v1")))
	s.Require().NoError(txn.Set(secondaryKey, []byte("v2")))

	_, err := s.store.SplitRegions(ctx, [][]byte{secondaryKey}, false, nil)
	s.Require().NoError(err)

	committer, err := txn.NewCommitter(1)
	s.Require().NoError(err)
	committer.SetPrimaryKey(primaryKey)

	s.Require().NoError(failpoint.Enable("tikvclient/twoPCRequestBatchSizeLimit", "return"))
	defer failpoint.Disable("tikvclient/twoPCRequestBatchSizeLimit")
	s.Require().NoError(failpoint.Enable("tikvclient/prewriteSecondarySleep", "return(2000)"))
	defer failpoint.Disable("tikvclient/prewriteSecondarySleep")

	prewriteErrCh := make(chan error, 1)
	go func() {
		prewriteErrCh <- committer.PrewriteAllMutations(ctx)
	}()

	var snapshotLocks []*txnlock.Lock
	var lastPrimaryType, lastSecondaryType kvrpcpb.Op
	lastLockCount := 0
	waitDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(waitDeadline) {
		locks, err := s.store.ScanLocks(ctx, primaryKey, []byte(""), math.MaxUint64)
		s.Require().NoError(err)
		lastLockCount = len(locks)
		lockByKey := make(map[string]*txnlock.Lock, len(locks))
		for _, l := range locks {
			lockByKey[string(l.Key)] = l
		}
		primary := lockByKey[string(primaryKey)]
		secondary := lockByKey[string(secondaryKey)]
		if primary != nil {
			lastPrimaryType = primary.LockType
		}
		if secondary != nil {
			lastSecondaryType = secondary.LockType
		}
		if primary != nil && secondary != nil {
			snapshotLocks = []*txnlock.Lock{primary, secondary}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(snapshotLocks) == 0 {
		s.T().Logf("last lock types primary=%v secondary=%v, last scan count=%d", lastPrimaryType, lastSecondaryType, lastLockCount)
	}
	s.Require().NotEmpty(snapshotLocks, "failed to capture locks before gc resolve")
	snapshotLocks[1].LockType = kvrpcpb.Op_PessimisticLock

	s.Require().NoError(<-prewriteErrCh)

	bo := tikv.NewGcResolveLockMaxBackoffer(ctx)
	loc, err := s.store.GetRegionCache().LocateKey(bo, snapshotLocks[0].Key)
	s.Require().NoError(err)
	resolved, err := s.store.GetLockResolver().BatchResolveLocks(bo, snapshotLocks, loc.Region)
	s.Require().NoError(err)
	s.True(resolved)

	commitTS, err := s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Require().NoError(err)
	committer.SetCommitTS(commitTS)
	commitErr := committer.CommitMutations(ctx)

	readTS, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Require().NoError(err)
	snapshot := s.store.GetSnapshot(readTS)

	v1, err1 := snapshot.Get(ctx, primaryKey)
	v2, err2 := snapshot.Get(ctx, secondaryKey)

	s.False((err1 == nil) != (err2 == nil), "inconsistent commit state: err1=%v err2=%v", err1, err2)

	if commitErr == nil {
		s.Require().NoError(err1)
		s.Require().NoError(err2)
		s.Equal([]byte("v1"), v1)
		s.Equal([]byte("v2"), v2)
	} else {
		s.Equal(tikverr.ErrNotExist, err1)
		s.Equal(tikverr.ErrNotExist, err2)
	}

	cleanupTxn := s.beginTxn()
	s.Require().NoError(cleanupTxn.Delete(primaryKey))
	s.Require().NoError(cleanupTxn.Delete(secondaryKey))
	s.Require().NoError(cleanupTxn.Commit(ctx))
}
