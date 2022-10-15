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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/tikv/client-go/v2/txnkv/txnutil"
)

const (
	TOKEN_PER_CORE = 64
)

type AdmissionManager struct {
	stop     uint64
	maxToken uint64
	notify   chan struct{}
	tokenCh  chan uint64
	mu       struct {
		sync.Mutex
		availableToken uint64
		sleep          bool
		heap           waiterHeap
	}
}

var (
	GlobalManager = NewAdmissionManager()
)

func NewAdmissionManager() *AdmissionManager {
	maxToken := runtime.NumCPU() * TOKEN_PER_CORE
	am := &AdmissionManager{
		stop:     0,
		maxToken: uint64(maxToken),
		notify:   make(chan struct{}),
		tokenCh:  make(chan uint64, maxToken),
		mu: struct {
			sync.Mutex
			availableToken uint64
			sleep          bool
			heap           waiterHeap
		}{
			availableToken: uint64(maxToken),
			sleep:          false,
			heap:           make(waiterHeap, 0, 256),
		},
	}
	go am.run()
	return am
}

func (am *AdmissionManager) Admit(created uint64, priority txnutil.Priority, requireToken uint64) *waiter {
	w := waiter{
		epoch:        created / 1000,
		priority:     priority,
		created:      created,
		admit:        make(chan struct{}),
		requireToken: requireToken,
		tokenCh:      am.tokenCh,
	}
	am.mu.Lock()
	heap.Push(&am.mu.heap, &w)
	notify := am.mu.sleep
	if notify {
		am.mu.sleep = false
	}
	am.mu.Unlock()
	if notify {
		am.notify <- struct{}{}
	}
	return &w
}

func (am *AdmissionManager) run() {
	// waitToken accept the released token from the tokenCh.
	waitToken := uint64(0)
	for {
		am.mu.Lock()
		if am.mu.heap.Len() == 0 {
			am.mu.sleep = true
			am.mu.Unlock()
			<-am.notify
			if atomic.LoadUint64(&am.stop) == 1 {
				return
			}
			continue
		}
		w := heap.Pop(&am.mu.heap).(*waiter)
		l := len(am.tokenCh)
		for i := 0; i < l; i++ {
			am.mu.availableToken += <-am.tokenCh
		}
		if waitToken != 0 {
			am.mu.availableToken += waitToken
			waitToken = 0
		}
		if am.mu.availableToken < w.requireToken {
			heap.Push(&am.mu.heap, w)
			am.mu.Unlock()
			waitToken = <-am.tokenCh
			continue
		}
		am.mu.availableToken -= w.requireToken
		am.mu.Unlock()
		w.admit <- struct{}{}
	}
}

func (am *AdmissionManager) Close() {
	atomic.StoreUint64(&am.stop, 1)
	close(am.notify)
}

func (am *AdmissionManager) GetAvailableToken() uint64 {
	am.mu.Lock()
	defer am.mu.Unlock()
	l := len(am.tokenCh)
	for i := 0; i < l; i++ {
		am.mu.availableToken += <-am.tokenCh
	}
	return am.mu.availableToken
}

type waiter struct {
	priority     txnutil.Priority
	created      uint64
	epoch        uint64
	admit        chan struct{}
	requireToken uint64
	tokenCh      chan<- uint64
}

func (w *waiter) Wait(ctx context.Context) {
	select {
	case <-ctx.Done():
		close(w.admit)
	case <-w.admit:
	}
}

func (w *waiter) Release() {
	w.tokenCh <- w.requireToken
}

type waiterHeap []*waiter

var _ heap.Interface = (*waiterHeap)(nil)

func (w waiterHeap) Len() int {
	return len(w)
}

func (w waiterHeap) Less(i, j int) bool {
	if w[i].epoch != w[j].epoch {
		return w[i].epoch < w[j].epoch
	}
	if w[i].priority != w[j].priority {
		switch w[i].priority {
		case txnutil.PriorityHigh:
			return true
		case txnutil.PriorityNormal:
			// if the priority of w[j] is either high or low.
			return w[j].priority != txnutil.PriorityHigh
		case txnutil.PriorityLow:
			return false
		}
	}
	return w[i].created < w[j].created
}

func (w waiterHeap) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}

func (w *waiterHeap) Push(wt any) {
	*w = append(*w, wt.(*waiter))
}

func (w *waiterHeap) Pop() any {
	old := *w
	n := len(old)
	x := old[n-1]
	*w = old[0 : n-1]
	return x
}
