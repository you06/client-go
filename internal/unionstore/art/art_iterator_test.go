package art

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIterateNodeCapacity(t *testing.T) {
	check := func(tree *Art, startKey, endKey []byte, startVal, endVal int) {
		// iter
		it, err := tree.Iter(startKey, endKey)
		assert.Nil(t, err)
		handles := make([]ArtMemKeyHandle, 0, endVal-startVal+1)
		for i := startVal; i <= endVal; i++ {
			assert.True(t, it.Valid(), i)
			assert.Equal(t, it.Key(), []byte{byte(i)})
			assert.Equal(t, it.Value(), []byte{byte(i)})
			handles = append(handles, it.Handle())
			assert.Nil(t, it.Next())
		}
		assert.False(t, it.Valid())
		assert.Error(t, it.Next())
		for i, handle := range handles {
			assert.Equal(t, tree.GetKeyByHandle(handle), []byte{byte(startVal + i)})
			val, valid := tree.GetValueByHandle(handle)
			assert.True(t, valid)
			assert.Equal(t, val, []byte{byte(startVal + i)})
		}
		// reverse iter
		it, err = tree.IterReverse(endKey, startKey)
		assert.Nil(t, err)
		handles = handles[:0]
		for i := endVal; i >= startVal; i-- {
			assert.True(t, it.Valid(), i)
			assert.Equal(t, it.Key(), []byte{byte(i)})
			assert.Equal(t, it.Value(), []byte{byte(i)})
			handles = append(handles, it.Handle())
			assert.Nil(t, it.Next())
		}
		assert.False(t, it.Valid())
		assert.Error(t, it.Next())
		for i, handle := range handles {
			assert.Equal(t, tree.GetKeyByHandle(handle), []byte{byte(endVal - i)})
			val, valid := tree.GetValueByHandle(handle)
			assert.True(t, valid)
			assert.Equal(t, val, []byte{byte(endVal - i)})
		}
	}
	for _, capacity := range []int{node4cap, node16cap, node48cap, node256cap} {
		tree := New()
		for i := 0; i < capacity; i++ {
			key := []byte{byte(i)}
			assert.Nil(t, tree.Set(key, key))
		}
		check(tree, nil, nil, 0, capacity-1)
		mid := capacity / 2
		check(tree, []byte{byte(mid)}, nil, mid, capacity-1) // lower bound is inclusive
		check(tree, nil, []byte{byte(mid)}, 0, mid-1)        // upper bound is exclusive
	}
}

func TestIterSeekLeaf(t *testing.T) {
	for _, capacity := range []int{node4cap, node16cap, node48cap, node256cap} {
		tree := New()
		for i := 0; i < capacity; i++ {
			key := []byte{byte(i)}
			assert.Nil(t, tree.Set(key, key))
		}
		for i := 0; i < capacity; i++ {
			key := []byte{byte(i)}
			it, err := tree.Iter(key, nil)
			assert.Nil(t, err)
			idxes, nodes := it.seek(key)
			assert.Greater(t, len(idxes), 0)
			assert.Equal(t, len(idxes), len(nodes))
			leafNode := nodes[len(nodes)-1].at(&tree.allocator, idxes[len(idxes)-1])
			assert.NotEqual(t, leafNode, nullArtNode)
			leaf := leafNode.leaf(&tree.allocator)
			assert.Equal(t, []byte(leaf.getKey()), key)
		}
	}
}

func TestMultiLevelIterate(t *testing.T) {
	tree := New()
	var keys [][]byte
	for i := 0; i < 20; i++ {
		key := make([]byte, i+1)
		keys = append(keys, key)
	}
	for _, key := range keys {
		assert.Nil(t, tree.Set(key, key))
	}
	// iter
	it, err := tree.Iter(nil, nil)
	assert.Nil(t, err)
	handles := make([]ArtMemKeyHandle, 0, len(keys))
	for _, key := range keys {
		assert.True(t, it.Valid())
		assert.Equal(t, it.Key(), key)
		assert.Equal(t, it.Value(), key)
		handles = append(handles, it.Handle())
		assert.Nil(t, it.Next())
	}
	assert.False(t, it.Valid())
	assert.Error(t, it.Next())
	for i, handle := range handles {
		assert.Equal(t, tree.GetKeyByHandle(handle), keys[i])
		val, valid := tree.GetValueByHandle(handle)
		assert.True(t, valid)
		assert.Equal(t, val, keys[i])
	}
	// reverse iter
	it, err = tree.IterReverse(nil, nil)
	assert.Nil(t, err)
	handles = handles[:0]
	for i := len(keys) - 1; i >= 0; i-- {
		assert.True(t, it.Valid())
		assert.Equal(t, it.Key(), keys[i])
		assert.Equal(t, it.Value(), keys[i])
		handles = append(handles, it.Handle())
		assert.Nil(t, it.Next())
	}
	assert.False(t, it.Valid())
	assert.Error(t, it.Next())
	for i, handle := range handles {
		assert.Equal(t, tree.GetKeyByHandle(handle), keys[len(keys)-i-1])
		val, valid := tree.GetValueByHandle(handle)
		assert.True(t, valid)
		assert.Equal(t, val, keys[len(keys)-i-1])
	}
}
