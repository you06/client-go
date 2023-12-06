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
	onFlushingMemDB *MemDB
	// flush the given memdb to stores, so that snapshot read can see the data.
	// before the flush is completed, the data will be read from memdb.
	// to avoid race, DO NOT WRITE the memdb in flush function.
	// once flush returns error, the transaction failed.
	flush func(*MemDB) error
}

// NewPrewriteAheadKVMemDBUnionStore builds a new unionStore with multi memdb.
func NewPrewriteAheadKVMemDBUnionStore(snapshot uSnapshot, worker int, flushHandle func(*MemDB) error) *PrewriteAheadKVMemDBUnionStore {
	unistore := &PrewriteAheadKVMemDBUnionStore{
		snapshot:        snapshot,
		memBuffer:       newMemDB(),
		onFlushingMemDB: newMemDB(),
		flush:           flushHandle,
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
	v, err := us.memBuffer.Get(k)
	if tikverr.IsErrNotFound(err) {
		v, err = us.onFlushingMemDB.Get(k)
	}
	us.RUnlock()
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

// Iter implements the Retriever interface.
func (us *PrewriteAheadKVMemDBUnionStore) Iter(k, upperBound []byte) (Iterator, error) {
	retrieverIt, err := us.snapshot.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}

	its, err := us.getIts(retrieverIt, func(memDB *MemDB) (Iterator, error) {
		return memDB.Iter(k, upperBound)
	})
	return NewMultiMemDBUnionIter(its, false)
}

// IterReverse implements the Retriever interface.
func (us *PrewriteAheadKVMemDBUnionStore) IterReverse(k, lowerBound []byte) (Iterator, error) {
	retrieverIt, err := us.snapshot.IterReverse(k, lowerBound)
	if err != nil {
		return nil, err
	}
	its, err := us.getIts(retrieverIt, func(memDB *MemDB) (Iterator, error) {
		return memDB.IterReverse(k, lowerBound)
	})
	return NewMultiMemDBUnionIter(its, false)
}

// MultiMemDBUnionIter is the iterator on an UnionStore.
type MultiMemDBUnionIter struct {
	// the index 0 is snapshot iterator, the higher index always overwrite the lower.
	its     []Iterator
	valids  []bool
	cur     int
	isValid bool
	reverse bool
}

func NewMultiMemDBUnionIter(its []Iterator, reverse bool) (*MultiMemDBUnionIter, error) {
	valids := make([]bool, len(its))
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

func (iter *MultiMemDBUnionIter) updateCur() error {
	iter.isValid = true
	for {
		if iter.cur >= 0 {
			if iter.reverse {
				if err := iter.its[iter.cur].Prev(); err != nil {
					return err
				}
			} else {
				if err := iter.its[iter.cur].Next(); err != nil {
					return err
				}
			}
		}
		iter.cur = -1
		for i := range iter.its {
			if !iter.valids[i] {
				continue
			}
			if iter.cur == -1 {
				iter.cur = i
				continue
			}
			cmp := bytes.Compare(iter.its[i].Key(), iter.its[iter.cur].Key())
			if iter.reverse {
				cmp = -cmp
			}
			if cmp < 0 {
				iter.cur = i
			}
		}
		if iter.cur == -1 {
			iter.isValid = false
			return nil
		}
		for i := range iter.its {
			if i == iter.cur {
				continue
			}
			cmp := bytes.Compare(iter.its[i].Key(), iter.its[iter.cur].Key())
			if iter.reverse {
				cmp = -cmp
			}
			if cmp == 0 {
				iter.valids[i] = true
			} else {
				iter.valids[i] = false
			}
		}
		if iter.its[iter.cur].Valid() {
			break
		}
		iter.valids[iter.cur] = false
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
	return iter.updateCur()
}

func (iter *MultiMemDBUnionIter) Close() {
}
