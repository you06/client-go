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
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func mockFlushHandle(store *MemDB) (FlushHandle, chan error) {
	ch := make(chan error)
	return func(m *MemDB) error {
		iter, err := m.Iter(nil, nil)
		if err != nil {
			return err
		}
		for iter.Valid() {
			err = store.Set(iter.Key(), iter.Value())
			if err != nil {
				return err
			}
			iter.Next()
		}
		iter.Close()
		return <-ch
	}, ch
}

func TestPrewriteAheadUnionStoreGetSet(t *testing.T) {
	store := newMemDB()
	flush, ch := mockFlushHandle(store)
	defer close(ch)
	us := NewPrewriteAheadKVMemDBUnionStore(&mockSnapshot{store}, flush)

	err := store.Set([]byte("1"), []byte("1"))
	require.Nil(t, err)
	v, err := us.Get(context.TODO(), []byte("1"))
	require.Nil(t, err)
	require.Equal(t, v, []byte("1"))
	err = us.GetMemBuffer().Set([]byte("1"), []byte("2"))
	require.Nil(t, err)
	v, err = us.Get(context.TODO(), []byte("1"))
	require.Nil(t, err)
	require.Equal(t, v, []byte("2"))
	require.Equal(t, us.GetMemBuffer().Size(), 2)
	require.Equal(t, us.GetMemBuffer().Len(), 1)

	// test flush
	us.flush()
	v, err = us.Get(context.TODO(), []byte("1"))
	require.Nil(t, err)
	require.Equal(t, v, []byte("2"))

	// flush done
	ch <- nil
	for {
		us.RLock()
		onFlushing := us.onFlushingMemBuffer
		us.RUnlock()
		if onFlushing == nil {
			break
		}
	}
	v, err = us.Get(context.TODO(), []byte("1"))
	require.Nil(t, err)
	require.Equal(t, v, []byte("2"))
}

func TestPrewriteAheadUnionStoreIter(t *testing.T) {
	once := func(i, j, k int) {
		store := newMemDB()
		flush, ch := mockFlushHandle(store)
		defer close(ch)
		us := NewPrewriteAheadKVMemDBUnionStore(&mockSnapshot{store}, flush)

		require.Nil(t, store.Set([]byte{byte(i)}, []byte{byte(i)}))
		require.Nil(t, us.GetMemBuffer().Set([]byte{byte(j)}, []byte{byte(j)}))
		us.flush()
		require.Nil(t, us.GetMemBuffer().Set([]byte{byte(k)}, []byte{byte(k)}))

		expects := []byte{1, 2, 3}
		//reverseExpects := []byte{3, 2, 1}

		fmt.Println(i, j, k)
		iter, err := us.Iter(nil, nil)
		require.Nil(t, err)
		_, ok := iter.(*MultiMemDBUnionIter)
		require.True(t, ok)
		for _, expect := range expects {
			require.True(t, iter.Valid())
			require.Equal(t, iter.Key(), []byte{expect})
			require.Equal(t, iter.Value(), []byte{expect})
			require.Nil(t, iter.Next())
		}
		require.False(t, iter.Valid())
		iter.Close()
	}
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			if i == j {
				continue
			}
			for k := 0; k < 3; k++ {
				if k == i || k == j {
					continue
				}
				once(i, j, k)
			}
		}
	}
}
