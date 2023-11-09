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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/oracle/oracles/pd_test.go
//

// Copyright 2019 PingCAP, Inc.
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

package oracles_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
)

func TestPDOracle_UntilExpired(t *testing.T) {
	lockAfter, lockExp := 10, 15
	o := oracles.NewEmptyPDOracle()
	start := time.Now()
	oracles.SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(start))
	lockTs := oracle.GoTimeToTS(start.Add(time.Duration(lockAfter)*time.Millisecond)) + 1
	waitTs := o.UntilExpired(lockTs, uint64(lockExp), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	assert.Equal(t, int64(lockAfter+lockExp), waitTs)
}

func TestPdOracle_GetStaleTimestamp(t *testing.T) {
	o := oracles.NewEmptyPDOracle()

	start := time.Now()
	oracles.SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(start))
	ts, err := o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, 10)
	assert.Nil(t, err)
	assert.WithinDuration(t, start.Add(-10*time.Second), oracle.GetTimeFromTS(ts), 2*time.Second)

	_, err = o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, 1e12)
	assert.NotNil(t, err)
	assert.Regexp(t, ".*invalid prevSecond.*", err.Error())

	_, err = o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, math.MaxUint64)
	assert.NotNil(t, err)
	assert.Regexp(t, ".*invalid prevSecond.*", err.Error())
}

func TestNonFutureStaleTSO(t *testing.T) {
	o := oracles.NewEmptyPDOracle()
	oracles.SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(time.Now()))
	for i := 0; i < 100; i++ {
		time.Sleep(10 * time.Millisecond) // sleep 10ms to limit the tso cast error within 5ms.
		now := time.Now()
		upperBound := now.Add(5 * time.Millisecond)

		closeCh := make(chan struct{})
		go func() {
			time.Sleep(100 * time.Microsecond)
			oracles.SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(now))
			close(closeCh)
		}()
	CHECK:
		for {
			select {
			case <-closeCh:
				break CHECK
			default:
				ts, err := o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, 0)
				assert.Nil(t, err)
				staleTime := oracle.GetTimeFromTS(ts)
				if staleTime.After(upperBound) && time.Since(now) < time.Millisecond {
					assert.Less(t, staleTime, upperBound, i)
					t.FailNow()
				}
			}
		}
	}
}
