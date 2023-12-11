// Copyright 2023 TiKV Authors
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

package unionstore

import (
	"bytes"
	"context"
	"sync"

	tikverr "github.com/tikv/client-go/v2/error"
)

// PrewriteAheadKVMemDBUnionStore is an in-memory Store with multi-memdb which contains a buffer for write and a
// snapshot for read.
type PrewriteAheadKVMemDBUnionStore struct {
	sync.RWMutex
	snapshot  uSnapshot
	memBuffer *MemDB
	// The memdb which is flushing to stores, should be overwritten by memBuffer when reading.
	onFlushingMemBuffer *MemDB
	// flush the given memdb to stores, so that snapshot read can see the data.
	// before the flush is completed, the data will be read from memdb.
	// to avoid race, DO NOT WRITE the memdb in flush function.
	// once flush returns error, the transaction failed.
	flush func(*MemDB) error
}

// NewPrewriteAheadKVMemDBUnionStore builds a new unionStore with multi memdb.
func NewPrewriteAheadKVMemDBUnionStore(snapshot uSnapshot, worker int, flushHandle func(*MemDB) error) *PrewriteAheadKVMemDBUnionStore {
	unistore := &PrewriteAheadKVMemDBUnionStore{
		snapshot:            snapshot,
		memBuffer:           newMemDB(),
		onFlushingMemBuffer: newMemDB(),
		flush:               flushHandle,
	}
	return unistore
}

// GetMemBuffer return the MemBuffer binding to this unionStore.
func (us *PrewriteAheadKVMemDBUnionStore) GetMemBuffer() *MemDB {
	return us.memBuffer
}

// Get implements the Retriever interface.
func (us *PrewriteAheadKVMemDBUnionStore) Get(ctx context.Context, k []byte) ([]byte, error) {
	us.RLock()
	memBuffer, onFlushingMemBuffer := us.memBuffer, us.onFlushingMemBuffer
	us.RUnlock()

	v, err := memBuffer.Get(k)
	if tikverr.IsErrNotFound(err) {
		v, err = onFlushingMemBuffer.Get(k)
	}
	if tikverr.IsErrNotFound(err) {
		v, err = us.snapshot.Get(ctx, k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}

func (us *PrewriteAheadKVMemDBUnionStore) getIts(memdb2it func(*MemDB) (Iterator, error)) (Iterator, Iterator, error) {
	us.RLock()
	memdb, onFlushingMemdb := us.memBuffer, us.onFlushingMemBuffer
	us.RUnlock()
	memdbIt, err := memdb2it(memdb)
	if err != nil {
		return nil, nil, err
	}
	onFlushingMemdbIt, err := memdb2it(onFlushingMemdb)
	if err != nil {
		return nil, nil, err
	}
	return memdbIt, onFlushingMemdbIt, nil
}

// Iter implements the Retriever interface.
func (us *PrewriteAheadKVMemDBUnionStore) Iter(k, upperBound []byte) (Iterator, error) {
	retrieverIt, err := us.snapshot.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	memdbIt, onFlushingMemdbIt, err := us.getIts(func(memDB *MemDB) (Iterator, error) {
		return memDB.Iter(k, upperBound)
	})
	if err != nil {
		return nil, err
	}
	return NewMultiMemDBUnionIter([3]Iterator{retrieverIt, onFlushingMemdbIt, memdbIt}, false)
}

// IterReverse implements the Retriever interface.
func (us *PrewriteAheadKVMemDBUnionStore) IterReverse(k, lowerBound []byte) (Iterator, error) {
	retrieverIt, err := us.snapshot.IterReverse(k, lowerBound)
	if err != nil {
		return nil, err
	}
	memdbIt, onFlushingMemdbIt, err := us.getIts(func(memDB *MemDB) (Iterator, error) {
		return memDB.IterReverse(k, lowerBound)
	})
	if err != nil {
		return nil, err
	}
	return NewMultiMemDBUnionIter([3]Iterator{retrieverIt, onFlushingMemdbIt, memdbIt}, true)
}

// MultiMemDBUnionIter is the iterator on an UnionStore.
type MultiMemDBUnionIter struct {
	// the index 0 is snapshot iterator, the higher index always overwrite the lower.
	its     [3]Iterator
	valids  [3]bool
	cur     int
	isValid bool
	reverse bool
}

func NewMultiMemDBUnionIter(its [3]Iterator, reverse bool) (*MultiMemDBUnionIter, error) {
	var valids [3]bool
	for i := range valids {
		valids[i] = its[i].Valid()
	}
	iter := &MultiMemDBUnionIter{
		its:     its,
		valids:  valids,
		cur:     -1,
		reverse: reverse,
	}
	if err := iter.updateCur(); err != nil {
		return nil, err
	}
	return iter, nil
}

func (iter *MultiMemDBUnionIter) next(i int) error {
	err := iter.its[i].Next()
	iter.valids[i] = iter.its[i].Valid()
	return err
}

// updateCur2 updates the cur between 2 iterators, return true if the cur non-delete.
func (iter *MultiMemDBUnionIter) updateCur2(cmp, i, j int) (bool, error) {
	switch cmp {
	case 0:
		if len(iter.its[j].Value()) == 0 {
			if err := iter.next(i); err != nil {
				return false, err
			}
			if err := iter.next(j); err != nil {
				return false, err
			}
		}
		if err := iter.next(i); err != nil {
			return false, err
		}
		iter.cur = j
	case 1:
		iter.cur = j
	case -1:
		iter.cur = i
	default:
		panic("unreachable")
	}
	return true, nil
}

func (iter *MultiMemDBUnionIter) updateCur() error {
	iter.isValid = true
	for {
		if !iter.valids[0] && !iter.valids[1] && !iter.valids[2] {
			break
		}

		// maybe use bit map? because 3 compare result can be saved in one int.
		var cmps [2]int
		cmps[0] = bytes.Compare(iter.its[0].Key(), iter.its[1].Key())
		cmps[1] = bytes.Compare(iter.its[1].Key(), iter.its[2].Key())

		if iter.reverse {
			for i := range cmps {
				cmps[i] = -cmps[i]
			}
		}

		switch cmps {
		case [2]int{-1, -1}, [2]int{-1, 0}:
			// first iter is the smallest.
			iter.cur = 0
			break
		case [2]int{-1, 1}:
			cmp2 := bytes.Compare(iter.its[0].Key(), iter.its[2].Key())
			if iter.reverse {
				cmp2 = -cmp2
			}
			stop, err := iter.updateCur2(cmp2, 0, 2)
			if err != nil {
				return err
			}
			if stop {
				break
			}
		case [2]int{0, -1}:
			stop, err := iter.updateCur2(0, 0, 1)
			if err != nil {
				return err
			}
			if stop {
				break
			}
		case [2]int{0, 1}, [2]int{1, 1}:
			iter.cur = 2
			break
		case [2]int{1, 0}:
			stop, err := iter.updateCur2(0, 1, 2)
			if err != nil {
				return err
			}
			if stop {
				break
			}
		case [2]int{1, -1}:
			iter.cur = 1
			break
		case [2]int{0, 0}:
			// need to skip delete record.
			if len(iter.its[2].Value()) == 0 {
				if err := iter.next(0); err != nil {
					return err
				}
				if err := iter.next(1); err != nil {
					return err
				}
				if err := iter.next(2); err != nil {
					return err
				}
				continue
			}
			if err := iter.next(0); err != nil {
				return err
			}
			if err := iter.next(1); err != nil {
				return err
			}
			iter.cur = 2
			break
		}
	}
	return nil
}

func (iter *MultiMemDBUnionIter) Valid() bool {
	return iter.isValid
}

func (iter *MultiMemDBUnionIter) Key() []byte {
	return iter.its[iter.cur].Key()
}

func (iter *MultiMemDBUnionIter) Value() []byte {
	return iter.its[iter.cur].Value()
}

func (iter *MultiMemDBUnionIter) Next() error {
	if err := iter.its[iter.cur].Next(); err != nil {
		return err
	}
	return iter.updateCur()
}

func (iter *MultiMemDBUnionIter) Close() {
}
