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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb_snapshot.go
//

// Copyright 2020 PingCAP, Inc.
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

package art

import (
	"context"

	tikverr "github.com/tikv/client-go/v2/error"
)

func (t *Art) getSnapshot() ARTCheckpoint {
	if len(t.stages) > 0 {
		return t.stages[0]
	}
	return t.checkpoint()
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (t *Art) SnapshotGetter() *memdbSnapGetter {
	return &memdbSnapGetter{
		db: t,
		cp: t.getSnapshot(),
	}
}

// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
func (t *Art) SnapshotIter(start, end []byte) *memdbSnapIter {
	inner, err := t.Iter(start, end)
	if err != nil {
		panic(err)
	}
	it := &memdbSnapIter{
		ArtIterator: inner,
		cp:          t.getSnapshot(),
	}
	return it
}

// SnapshotIterReverse returns a reverse Iterator for a snapshot of MemBuffer.
func (t *Art) SnapshotIterReverse(k, lowerBound []byte) *memdbSnapIter {
	inner, err := t.IterReverse(k, lowerBound)
	if err != nil {
		panic(err)
	}
	it := &memdbSnapIter{
		ArtIterator: inner,
		cp:          t.getSnapshot(),
	}
	return it
}

type memdbSnapGetter struct {
	db *Art
	cp ARTCheckpoint
}

func (snap *memdbSnapGetter) Get(ctx context.Context, key []byte) ([]byte, error) {
	addr, lf := snap.db.search(key)
	if addr.isNull() {
		return nil, tikverr.ErrNotExist
	}
	if lf.vAddr.isNull() {
		// A flags only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	v, ok := snap.db.getSnapshotValue(lf.vAddr, &snap.cp)
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}

type memdbSnapIter struct {
	*ArtIterator
	value []byte
	cp    ARTCheckpoint
}

func (i *memdbSnapIter) Value() []byte {
	return i.value
}

func (i *memdbSnapIter) Next() error {
	//i.value = nil
	//for i.Valid() {
	//	if err := i.MemdbIterator.Next(); err != nil {
	//		return err
	//	}
	//	if i.setValue() {
	//		return nil
	//	}
	//}
	return nil
}

func (i *memdbSnapIter) setValue() bool {
	//if !i.Valid() {
	//	return false
	//}
	//if v, ok := i.db.vlog.getSnapshotValue(i.curr.vptr, &i.cp); ok {
	//	i.value = v
	//	return true
	//}
	return false
}

func (i *memdbSnapIter) init() {
	//if i.reverse {
	//	if len(i.end) == 0 {
	//		i.seekToLast()
	//	} else {
	//		i.seek(i.end)
	//	}
	//} else {
	//	if len(i.start) == 0 {
	//		i.seekToFirst()
	//	} else {
	//		i.seek(i.start)
	//	}
	//}
	//
	//if !i.setValue() {
	//	err := i.Next()
	//	_ = err // memdbIterator will never fail
	//}
}
