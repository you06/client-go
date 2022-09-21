// Copyright 2022 TiKV Authors
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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/oracle/oracles/pd.go
//

// Copyright 2016 PingCAP, Inc.
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

package oracles

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/v2/oracle"
)

type ClockFn func() uint64

type hlcFuture struct {
	ctx context.Context
	h   *HLCClock
}

func (f *hlcFuture) Wait() (uint64, error) {
	return f.h.GetTimestamp(f.ctx, &oracle.Option{})
}

type HLCClock struct {
	// the clock which produces physical time.
	clock ClockFn
	// time is the combine of last physical and logical time
	time uint64
}

// NewHLCClockWithNow return the common used HLCClock.
func NewHLCClockWithNow() oracle.Oracle {
	return NewHLCClock(func() uint64 {
		return uint64(time.Now().UnixMilli())
	})
}

// NewHLCClock creates an HLCClock with given clock.
func NewHLCClock(clock ClockFn) oracle.Oracle {
	return &HLCClock{
		clock: clock,
		time:  0,
	}
}

func (h *HLCClock) GetTimestamp(context.Context, *oracle.Option) (uint64, error) {
	pt := h.clock()
	var ts uint64
	for {
		o := atomic.LoadUint64(&h.time)
		t, c := extractPhysicalAndLogical(o)
		if t >= pt {
			c++
		} else {
			t = pt
			c = 0
		}
		ts = oracle.ComposeTS(int64(t), int64(c))
		if atomic.CompareAndSwapUint64(&h.time, o, ts) {
			return ts, nil
		}
	}
}

func (h *HLCClock) OnMsg(m uint64) {
	pt := h.clock()
	mp, ml := extractPhysicalAndLogical(m)
	for {
		o := atomic.LoadUint64(&h.time)
		t, c := extractPhysicalAndLogical(o)
		nt := max3(pt, mp, t)
		if t == nt && nt == mp {
			c = max(c, ml) + 1
		} else if nt == t {
			c++
		} else if nt == mp {
			c = ml + 1
		} else {
			c = 0
		}
		ts := oracle.ComposeTS(int64(nt), int64(c))
		if atomic.CompareAndSwapUint64(&h.time, o, ts) {
			return
		}
	}
}

func (h *HLCClock) GetTimestampAsync(ctx context.Context, _ *oracle.Option) oracle.Future {
	return &hlcFuture{
		ctx: ctx,
		h:   h,
	}
}

func (h *HLCClock) GetLowResolutionTimestamp(ctx context.Context, _ *oracle.Option) (uint64, error) {
	return h.GetTimestamp(ctx, nil)
}

func (h *HLCClock) GetLowResolutionTimestampAsync(ctx context.Context, _ *oracle.Option) oracle.Future {
	return h.GetTimestampAsync(ctx, nil)
}

// GetStaleTimestamp return physical, never used now
func (h *HLCClock) GetStaleTimestamp(ctx context.Context, txnScope string, prevSecond uint64) (ts uint64, err error) {
	return oracle.GoTimeToTS(time.Now().Add(-time.Second * time.Duration(prevSecond))), nil
}

func (h *HLCClock) IsExpired(lockTimestamp, TTL uint64, _ *oracle.Option) bool {
	nowTS, _ := h.GetTimestamp(context.Background(), nil)
	lock, now := oracle.GetTimeFromTS(lockTimestamp), oracle.GetTimeFromTS(nowTS)
	expire := lock.Add(time.Duration(TTL) * time.Millisecond)
	return !now.Before(expire)
}

func (h *HLCClock) UntilExpired(lockTimeStamp, TTL uint64, _ *oracle.Option) int64 {
	nowTS, _ := h.GetTimestamp(context.Background(), nil)
	now := oracle.GetTimeFromTS(nowTS)
	return oracle.ExtractPhysical(lockTimeStamp) + int64(TTL) - oracle.GetPhysical(now)
}

func (h *HLCClock) Close() {}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func max3(a, b, c uint64) uint64 {
	if a > b {
		if a > c {
			return a
		} else {
			return c
		}
	}
	if b > c {
		return b
	}
	return c
}

func extractPhysicalAndLogical(t uint64) (uint64, uint64) {
	return uint64(oracle.ExtractPhysical(t)), uint64(oracle.ExtractLogical(t))
}
