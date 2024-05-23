package unionstore

import (
	art "github.com/plar/go-adaptive-radix-tree"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"math"
)

type ArtMemDB struct {
	len, size       int
	stages          []int // donot support stages by now
	tree            art.Tree
	entrySizeLimit  uint64
	bufferSizeLimit uint64
}

type FlagValue struct {
	value []byte
	flags kv.KeyFlags
}

func newArtMemDB() *ArtMemDB {
	artTree := art.New()
	artTree.Size()
	return &ArtMemDB{
		tree:            artTree,
		entrySizeLimit:  math.MaxUint64,
		bufferSizeLimit: math.MaxUint64,
	}
}

func (a *ArtMemDB) Bounds() ([]byte, []byte) {
	lower, _ := a.tree.MinimumKey()
	upper, _ := a.tree.MaximumKey()
	return lower, upper
}

func (a *ArtMemDB) Set(key []byte, value []byte) error {
	cpValues := make([]byte, len(value))
	copy(cpValues, value)
	flagVal := &FlagValue{
		value: cpValues,
	}
	old, updated := a.tree.Insert(key, flagVal)
	a.len++
	a.size += len(key) + len(value)
	if old != nil && updated {
		a.len--
		a.size -= len(key) + len(old.(*FlagValue).value)
	}
	return nil
}

func (a *ArtMemDB) SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error {
	cpValues := make([]byte, len(value))
	copy(cpValues, value)
	flagVal := &FlagValue{
		value: cpValues,
		flags: kv.ApplyFlagsOps(0, ops...),
	}
	old, updated := a.tree.Insert(key, flagVal)
	a.len++
	a.size += len(key) + len(value)
	if old != nil && updated {
		a.len--
		a.size -= len(key) + len(old.(*FlagValue).value)
	}
	return nil
}

func (a *ArtMemDB) Delete(key []byte) error {
	flagVal, found := a.tree.Search(key)
	if found {
		flagVal.(*FlagValue).value = tombstone
		return nil
	}
	flagVal = &FlagValue{
		value: tombstone,
	}
	old, updated := a.tree.Insert(key, flagVal)
	a.len++
	a.size += len(key)
	if old != nil && updated {
		a.len--
		a.size -= len(key) + len(old.(*FlagValue).value)
	}
	return nil
}

func (a *ArtMemDB) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	val, found := a.tree.Search(key)
	if found {
		flagVal := val.(*FlagValue)
		flagVal.value = tombstone
		flagVal.flags = kv.ApplyFlagsOps(flagVal.flags, ops...)
		return nil
	}
	flagVal := &FlagValue{
		value: tombstone,
		flags: kv.ApplyFlagsOps(0, ops...),
	}
	old, updated := a.tree.Insert(key, flagVal)
	a.len++
	a.size += len(key)
	if old != nil && updated {
		a.len--
		a.size -= len(key) + len(old.(*FlagValue).value)
	}
	return nil
}

func (a *ArtMemDB) UpdateFlags(key []byte, ops ...kv.FlagsOp) {
	val, found := a.tree.Search(key)
	if !found {
		return
	}
	flagVal := val.(*FlagValue)
	flagVal.flags = kv.ApplyFlagsOps(flagVal.flags, ops...)
}

func (a *ArtMemDB) Get(key []byte) ([]byte, error) {
	flagVal, found := a.tree.Search(key)
	if !found {
		return nil, tikverr.ErrNotExist
	}
	return flagVal.(*FlagValue).value, nil
}

func (a *ArtMemDB) GetFlags(key []byte) (kv.KeyFlags, error) {
	flagVal, found := a.tree.Search(key)
	if !found {
		return 0, tikverr.ErrNotExist
	}
	return flagVal.(*FlagValue).flags, nil
}

func (a *ArtMemDB) Dirty() bool {
	return a.tree.Size() > 0
}

func (a *ArtMemDB) Len() int {
	return a.len
}

func (a *ArtMemDB) Size() int {
	return a.size
}

func (a *ArtMemDB) Mem() uint64 {
	return uint64(a.size)
}

func (a *ArtMemDB) Staging() int {
	return 0
}

func (a *ArtMemDB) Release(int) {}

func (a *ArtMemDB) Cleanup(int) {}

var _ Iterator = &ArtMemDBIterator{}

type ArtMemDBIterator struct {
	from, to []byte
	inner    art.Iterator
	cur      art.Node
	valid    bool
}

func (a *ArtMemDB) Iter(k []byte, upperBound []byte) (Iterator, error) {
	return a.IterWithFlags(k, upperBound), nil
}

func (a *ArtMemDB) IterWithFlags(k []byte, upperBound []byte) *ArtMemDBIterator {
	inner := a.tree.Iterator(art.TraverseAll)
	iterator := &ArtMemDBIterator{from: k, to: upperBound, inner: inner, cur: nil, valid: true}
	iterator.Next()
	return iterator
}

func (a *ArtMemDBIterator) Valid() bool {
	return a.valid
}

func (a *ArtMemDBIterator) Next() error {
	var err error
	for {
		a.cur, err = a.inner.Next()
		if err != nil {
			a.valid = false
			return nil
		}
		currKey := []byte(a.cur.Key())
		if currKey == nil {
			continue
		}
		if a.to != nil && kv.CmpKey(currKey, a.to) > 0 {
			return nil
		}
		if a.from != nil && kv.CmpKey(currKey, a.from) < 0 {
			continue
		}
		return nil
	}
}

func (a *ArtMemDBIterator) Flags() kv.KeyFlags {
	keyFlags := a.cur.Value().(*FlagValue)
	return keyFlags.flags
}

func (a *ArtMemDBIterator) HasValue() bool {
	val := a.cur.Value()
	if val == nil {
		return false
	}
	return val.(*FlagValue).value != nil
}

func (a *ArtMemDBIterator) Key() []byte {
	return a.cur.Key()
}

func (a *ArtMemDBIterator) Value() []byte {
	return a.cur.Value().(*FlagValue).value
}

func (a *ArtMemDBIterator) Close() {}
