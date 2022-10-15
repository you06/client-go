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

package admission

import (
	"container/heap"
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/txnkv/txnutil"
)

func newWaiterForTest(priority txnutil.Priority, created, epoch uint64) waiter {
	return waiter{
		priority:     priority,
		created:      created,
		epoch:        epoch,
		admit:        nil,
		requireToken: 0,
		tokenCh:      nil,
	}
}

func TestHeapBasic(t *testing.T) {
	assert := assert.New(t)

	wh := waiterHeap(nil)
	heap.Init(&wh)
	waiters := []waiter{
		newWaiterForTest(txnutil.PriorityHigh, 10010, 10),
		newWaiterForTest(txnutil.PriorityHigh, 10000, 10),
		newWaiterForTest(txnutil.PriorityHigh, 9990, 9),
		newWaiterForTest(txnutil.PriorityNormal, 9980, 9),
		newWaiterForTest(txnutil.PriorityLow, 9970, 9),
	}
	expected := []uint64{9990, 9980, 9970, 10000, 10010}
	for i := 0; i < len(waiters); i++ {
		heap.Push(&wh, &waiters[i])
	}
	for _, e := range expected {
		assert.Equal(e, heap.Pop(&wh).(*waiter).created)
	}
}

func TestAdmit(t *testing.T) {
	assert := assert.New(t)

	am := NewAdmissionManager()
	defer am.Close()

	maxToken := am.GetAvailableToken()

	w := am.Admit(1, txnutil.PriorityHigh, 1)
	w.Wait(context.Background())
	assert.Equal(maxToken-1, am.GetAvailableToken())
	w.Release()
	assert.Equal(maxToken, am.GetAvailableToken())

	// token full
	waiters := make([]*waiter, 0, maxToken)
	for i := 0; i < int(maxToken); i++ {
		waiters = append(waiters, am.Admit(1, txnutil.PriorityHigh, 1))
		waiters[i].Wait(context.Background())
	}
	assert.Equal(uint64(0), am.GetAvailableToken())

	ch := make(chan uint64)
	signal := uint64(0)
	go func() {
		w := am.Admit(1, txnutil.PriorityHigh, 1)
		// should wait here until token released
		w.Wait(context.Background())
		ch <- atomic.LoadUint64(&signal)
		w.Release()
		ch <- math.MaxUint64
	}()
	time.Sleep(50 * time.Millisecond)
	atomic.StoreUint64(&signal, 1)
	waiters[0].Release()
	assert.Equal(uint64(1), <-ch)

	for i := 1; i < int(maxToken); i++ {
		waiters[i].Release()
	}
	<-ch
}
