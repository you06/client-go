package art

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIterateNode4(t *testing.T) {
	tree := New()
	for i := 0; i < node4cap; i++ {
		key := []byte{byte(i)}
		tree.Set(key, key)
	}
	it, err := tree.Iter(nil, nil)
	assert.Nil(t, err)
	for i := 0; i < node4cap; i++ {
		assert.True(t, it.Valid())
		assert.Equal(t, it.Key(), []byte{byte(i)})
		assert.Equal(t, it.Value(), []byte{byte(i)})
		assert.Nil(t, it.Next())
	}
	assert.False(t, it.Valid())
	assert.Error(t, it.Next())
}

func TestIterateNode16(t *testing.T) {
	tree := New()
	for i := 0; i < node16cap; i++ {
		key := []byte{byte(i)}
		tree.Set(key, key)
	}
	it, err := tree.Iter(nil, nil)
	assert.Nil(t, err)
	for i := 0; i < node16cap; i++ {
		assert.True(t, it.Valid())
		assert.Equal(t, it.Key(), []byte{byte(i)})
		assert.Equal(t, it.Value(), []byte{byte(i)})
		assert.Nil(t, it.Next())
	}
	assert.False(t, it.Valid())
	assert.Error(t, it.Next())
}

func TestIterateNode48(t *testing.T) {
	tree := New()
	for i := 0; i < node48cap; i++ {
		key := []byte{byte(i)}
		tree.Set(key, key)
	}
	it, err := tree.Iter(nil, nil)
	assert.Nil(t, err)
	for i := 0; i < node48cap; i++ {
		assert.True(t, it.Valid())
		assert.Equal(t, it.Key(), []byte{byte(i)})
		assert.Equal(t, it.Value(), []byte{byte(i)})
		assert.Nil(t, it.Next())
	}
	assert.False(t, it.Valid())
	assert.Error(t, it.Next())
}

func TestIterateNode256(t *testing.T) {
	tree := New()
	for i := 0; i < node256cap; i++ {
		key := []byte{byte(i)}
		tree.Set(key, key)
	}
	it, err := tree.Iter(nil, nil)
	assert.Nil(t, err)
	for i := 0; i < node256cap; i++ {
		assert.True(t, it.Valid())
		assert.Equal(t, it.Key(), []byte{byte(i)})
		assert.Equal(t, it.Value(), []byte{byte(i)})
		assert.Nil(t, it.Next())
	}
	assert.False(t, it.Valid())
	assert.Error(t, it.Next())
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
	assert.Nil(t, err)
	for _, key := range keys {
		assert.True(t, it.Valid())
		assert.Equal(t, it.Key(), key)
		assert.Equal(t, it.Value(), key)
		assert.Nil(t, it.Next())
	}
	assert.False(t, it.Valid())
	assert.Error(t, it.Next())
}
