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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb_bench_test.go
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

package unionstore

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	tikverr "github.com/tikv/client-go/v2/error"
)

const (
	keySize   = 16
	valueSize = 128
)

func BenchmarkLargeIndex(b *testing.B) {
	buf := make([][valueSize]byte, 10000000)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}
	db := newMemDB()
	b.ResetTimer()

	for i := range buf {
		db.Set(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkPut(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := newMemDB()
	b.ResetTimer()

	for i := range buf {
		p.Set(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkPutArt(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := newArtMemDB()
	b.ResetTimer()

	for i := range buf {
		p.Set(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkPutHashMap(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := make(map[string][]byte, 256)
	b.ResetTimer()

	for i := range buf {
		p[string(buf[i][:keySize])] = buf[i][:]
	}
}

func BenchmarkPutRandom(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	p := newMemDB()
	b.ResetTimer()

	for i := range buf {
		p.Set(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkPutRandomArt(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	p := newArtMemDB()
	b.ResetTimer()

	for i := range buf {
		p.Set(buf[i][:keySize], buf[i][:])
	}
}

func BenchmarkPutRandomHashMap(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	p := make(map[string][]byte, 256)
	b.ResetTimer()

	for i := range buf {
		p[string(buf[i][:keySize])] = buf[i][:]
	}
}

func BenchmarkGet(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := newMemDB()
	for i := range buf {
		p.Set(buf[i][:keySize], buf[i][:])
	}

	b.ResetTimer()
	for i := range buf {
		p.Get(buf[i][:keySize])
	}
}

func BenchmarkGetRandom(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	p := newMemDB()
	for i := range buf {
		p.Set(buf[i][:keySize], buf[i][:])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Get(buf[i][:keySize])
	}
}

var opCnt = 100000

func BenchmarkMemDbBufferSequential(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	buffer := newMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferSequentialArt(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	buffer := newArtMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

type haspMapMemDB struct {
	inner map[string][]byte
}

func newHaspMapMemDB() *haspMapMemDB {
	return &haspMapMemDB{inner: make(map[string][]byte, 256)}
}

func (m *haspMapMemDB) Set(key, value []byte) error {
	m.inner[string(key)] = value
	return nil
}

func (m *haspMapMemDB) Get(key []byte) ([]byte, error) {
	val, ok := m.inner[string(key)]
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return val, nil
}

func (m *haspMapMemDB) Iter(_ []byte, _ []byte) (Iterator, error) {
	return nil, errors.New("not implemented")
}

func BenchmarkMemDbBufferSequentialHashMap(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	buffer := newHaspMapMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferRandom(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	shuffle(data)
	buffer := newMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferRandomArt(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	shuffle(data)
	buffer := newArtMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferRandomHashMap(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeInt(i)
	}
	shuffle(data)
	buffer := newHaspMapMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferLongKeySequential(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeIntLong(i)
	}
	buffer := newMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferLongKeySequentialArt(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeIntLong(i)
	}
	buffer := newArtMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferLongKeySequentialHashMap(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeIntLong(i)
	}
	buffer := newHaspMapMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferLongKeyRandom(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeIntLong(i)
	}
	shuffle(data)
	buffer := newMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferLongKeyRandomArt(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeIntLong(i)
	}
	shuffle(data)
	buffer := newArtMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbBufferLongKeyRandomHashMap(b *testing.B) {
	data := make([][]byte, opCnt)
	for i := 0; i < opCnt; i++ {
		data[i] = encodeIntLong(i)
	}
	shuffle(data)
	buffer := newHaspMapMemDB()
	benchmarkSetGet(b, buffer, data)
	b.ReportAllocs()
}

func BenchmarkMemDbIter(b *testing.B) {
	buffer := newMemDB()
	benchIterator(b, buffer)
	b.ReportAllocs()
}

func BenchmarkMemDbIterArt(b *testing.B) {
	buffer := newArtMemDB()
	benchIterator(b, buffer)
	b.ReportAllocs()
}

func BenchmarkMemDbCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		newMemDB()
	}
	b.ReportAllocs()
}

func shuffle(slc [][]byte) {
	N := len(slc)
	for i := 0; i < N; i++ {
		// choose index uniformly in [i, N-1]
		r := i + rand.Intn(N-i)
		slc[r], slc[i] = slc[i], slc[r]
	}
}

type iMemDB interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Iter([]byte, []byte) (Iterator, error)
}

func benchmarkSetGet(b *testing.B, buffer iMemDB, data [][]byte) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range data {
			buffer.Set(k, k)
		}
		for _, k := range data {
			buffer.Get(k)
		}
	}
}

func benchIterator(b *testing.B, buffer iMemDB) {
	for k := 0; k < opCnt; k++ {
		buffer.Set(encodeInt(k), encodeInt(k))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := buffer.Iter(nil, nil)
		if err != nil {
			b.Error(err)
		}
		for iter.Valid() {
			iter.Next()
		}
		iter.Close()
	}
}

func BenchmarkReadAfterWriteRBT(b *testing.B) {
	buf := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		key := []byte{byte(i)}
		buf[i] = key
	}
	tree := newMemDB()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.set(buf[i], buf[i])
		v, _ := tree.Get(buf[i])
		assert.Equal(b, v, buf[i])
	}
}

func BenchmarkReadAfterWriteArtNoArena(b *testing.B) {
	buf := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		key := []byte{byte(i)}
		buf[i] = key
	}
	tree := newArtMemDB()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Set(buf[i], buf[i])
		v, _ := tree.Get(buf[i])
		assert.Equal(b, v, buf[i])
	}
}
