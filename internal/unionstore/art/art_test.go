package art

import (
	"testing"

	"github.com/stretchr/testify/assert"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestSimple(t *testing.T) {
	tree := New()
	for i := 0; i < 256; i++ {
		// key := []byte(strconv.Itoa(i))
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

// func BenchmarkTraverse(b *testing.B) {
// 	buf := make([][]byte, b.N)
// 	for i := range buf {
// 		buf[i] = []byte(strconv.Itoa(i))
// 	}

// 	art := New()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		art.traverse(buf[i], true)
// 	}
// }

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
