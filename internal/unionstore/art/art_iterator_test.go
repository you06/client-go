package art

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIterateNodeCapacity(t *testing.T) {
	for _, capacity := range []int{node4cap, node16cap, node48cap, node256cap} {
		tree := New()
		for i := 0; i < capacity; i++ {
			key := []byte{byte(i)}
			tree.Set(key, key)
		}
		it, err := tree.Iter(nil, nil)
		handles := make([]ArtMemKeyHandle, 0, capacity)
		assert.Nil(t, err)
		for i := 0; i < capacity; i++ {
			assert.True(t, it.Valid())
			assert.Equal(t, it.Key(), []byte{byte(i)})
			assert.Equal(t, it.Value(), []byte{byte(i)})
			handles = append(handles, it.Handle())
			assert.Nil(t, it.Next())
		}
		assert.False(t, it.Valid())
		assert.Error(t, it.Next())
		for i, handle := range handles {
			assert.Equal(t, tree.GetKeyByHandle(handle), []byte{byte(i)})
			val, valid := tree.GetValueByHandle(handle)
			assert.True(t, valid)
			assert.Equal(t, val, []byte{byte(i)})
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
		tree.Set(key, key)
	}
	it, err := tree.Iter(nil, nil)
	handles := make([]ArtMemKeyHandle, 0, len(keys))
	assert.Nil(t, err)
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
}
