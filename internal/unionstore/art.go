// Copyright 2024 TiKV Authors
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

//nolint:unused
package unionstore

import (
	"math"
	"sync"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

type ART struct {
	sync.RWMutex
	skipMutex       bool
	allocator       artAllocator
	root            artNode
	stages          []MemDBCheckpoint
	vlogInvalid     bool
	dirty           bool
	entrySizeLimit  uint64
	bufferSizeLimit uint64
	len             int
	size            int
}

func newART() *ART {
	var t ART
	t.root = nullArtNode
	t.stages = make([]MemDBCheckpoint, 0, 2)
	t.entrySizeLimit = math.MaxUint64
	t.bufferSizeLimit = math.MaxUint64
	t.allocator.nodeAllocator.freeNode4 = make([]memdbArenaAddr, 0, 1<<4)
	t.allocator.nodeAllocator.freeNode16 = make([]memdbArenaAddr, 0, 1<<3)
	t.allocator.nodeAllocator.freeNode48 = make([]memdbArenaAddr, 0, 1<<2)
	return &t
}

func (t *ART) Set(key, value []byte) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return t.set(key, value, nil)
}

// SetWithFlags put key-value into the last active staging buffer with the given KeyFlags.
func (t *ART) SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return t.set(key, value, ops)
}

func (t *ART) UpdateFlags(key []byte, ops ...kv.FlagsOp) {
	_ = t.set(key, nil, ops)
}

func (t *ART) Delete(key []byte) error {
	return t.set(key, tombstone, nil)
}

func (t *ART) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	return t.set(key, tombstone, ops)
}

func (t *ART) Get(key []byte) ([]byte, error) {
	panic("unimplemented")
}

// GetFlags returns the latest flags associated with key.
func (t *ART) GetFlags(key []byte) (kv.KeyFlags, error) {
	panic("unimplemented")
}

func (t *ART) set(key artKey, value []byte, ops []kv.FlagsOp) error {
	panic("unimplemented")
}

func (t *ART) search(key artKey) (memdbArenaAddr, *artLeaf) {
	panic("unimplemented")
}

func (t *ART) Dirty() bool {
	panic("unimplemented")
}

// Mem returns the memory usage of MemBuffer.
func (t *ART) Mem() uint64 {
	panic("unimplemented")
}

// Len returns the count of entries in the MemBuffer.
func (t *ART) Len() int {
	panic("unimplemented")
}

// Size returns the size of the MemBuffer.
func (t *ART) Size() int {
	panic("unimplemented")
}

func (t *ART) checkpoint() MemDBCheckpoint {
	panic("unimplemented")
}

func (t *ART) revertNode(hdr *memdbVlogHdr) {
	panic("unimplemented")
}

func (t *ART) inspectNode(addr memdbArenaAddr) (*artLeaf, memdbArenaAddr) {
	panic("unimplemented")
}

// Checkpoint returns a checkpoint of ART.
func (t *ART) Checkpoint() *MemDBCheckpoint {
	panic("unimplemented")
}

// RevertToCheckpoint reverts the ART to the checkpoint.
func (t *ART) RevertToCheckpoint(cp *MemDBCheckpoint) {
	panic("unimplemented")
}

func (t *ART) Stages() []MemDBCheckpoint {
	panic("unimplemented")
}

func (t *ART) Staging() int {
	panic("unimplemented")
}

func (t *ART) Release(h int) {
	panic("unimplemented")
}

func (t *ART) Cleanup(h int) {
	panic("unimplemented")
}

func (t *ART) revertToCheckpoint(cp *MemDBCheckpoint) {
	panic("unimplemented")
}

func (t *ART) moveBackCursor(cursor *MemDBCheckpoint, hdr *memdbVlogHdr) {
	panic("unimplemented")
}

func (t *ART) truncate(snap *MemDBCheckpoint) {
	panic("unimplemented")
}

// DiscardValues releases the memory used by all values.
// NOTE: any operation need value will panic after this function.
func (t *ART) DiscardValues() {
	panic("unimplemented")
}

// InspectStage used to inspect the value updates in the given stage.
func (t *ART) InspectStage(handle int, f func([]byte, kv.KeyFlags, []byte)) {
	panic("unimplemented")
}

// SelectValueHistory select the latest value which makes `predicate` returns true from the modification history.
func (t *ART) SelectValueHistory(key []byte, predicate func(value []byte) bool) ([]byte, error) {
	panic("unimplemented")
}

func (t *ART) SetMemoryFootprintChangeHook(fn func(uint64)) {
	panic("unimplemented")
}

// MemHookSet implements the MemBuffer interface.
func (t *ART) MemHookSet() bool {
	panic("unimplemented")
}

// GetKeyByHandle returns key by handle.
func (t *ART) GetKeyByHandle(handle MemKeyHandle) []byte {
	panic("unimplemented")
}

// GetValueByHandle returns value by handle.
func (t *ART) GetValueByHandle(handle MemKeyHandle) ([]byte, bool) {
	panic("unimplemented")
}

func (t *ART) SetEntrySizeLimit(entryLimit, bufferLimit uint64) {
	panic("unimplemented")
}

func (t *ART) SetSkipMutex(skip bool) {
	panic("unimplemented")
}

func (t *ART) RemoveFromBuffer(key []byte) {
	panic("unimplemented")
}
