package art

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestSimple(t *testing.T) {
	tree := New()
	for i := 0; i < 256; i++ {
		key := []byte{byte(i)}
		_, err := tree.Get(key)
		assert.Equal(t, err, tikverr.ErrNotExist)
		err = tree.Set(key, key)
		assert.Nil(t, err)
		val, err := tree.Get(key)
		assert.Nil(t, err, i)
		assert.Equal(t, val, key, i)
	}
}

func TestSubNode(t *testing.T) {
	tree := New()
	assert.Nil(t, tree.Set([]byte("a"), []byte("a")))
	assert.Nil(t, tree.Set([]byte("aa"), []byte("aa")))
	assert.Nil(t, tree.Set([]byte("aaa"), []byte("aaa")))
	v, err := tree.Get([]byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("a"))
	v, err = tree.Get([]byte("aa"))
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("aa"))
	v, err = tree.Get([]byte("aaa"))
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("aaa"))
}

func BenchmarkReadAfterWriteArt(b *testing.B) {
	buf := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		key := []byte{byte(i)}
		buf[i] = key
	}
	tree := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Set(buf[i], buf[i])
		v, _ := tree.Get(buf[i])
		assert.Equal(b, v, buf[i])
	}
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%010d", n))
}

func TestBenchKey(t *testing.T) {
	buffer := New()
	cnt := 100000
	for k := 0; k < cnt; k++ {
		key, value := encodeInt(k), encodeInt(k)
		buffer.Set(key, value)
	}
	for k := 0; k < cnt; k++ {
		v, err := buffer.Get(encodeInt(k))
		assert.Nil(t, err, k)
		assert.Equal(t, v, encodeInt(k))
	}
}

func TestLeafWithCommonPrefix(t *testing.T) {
	tree := New()
	tree.Set([]byte{1, 1, 1}, []byte{1, 1, 1})
	tree.Set([]byte{1, 1, 2}, []byte{1, 1, 2})
	v, err := tree.Get([]byte{1, 1, 1})
	assert.Nil(t, err)
	assert.Equal(t, v, []byte{1, 1, 1})
	v, err = tree.Get([]byte{1, 1, 2})
	assert.Nil(t, err)
	assert.Equal(t, v, []byte{1, 1, 2})
}
