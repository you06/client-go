package art

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimple(t *testing.T) {
	tree := New()
	for i := 0; i < 4; i++ {
		key := []byte(strconv.Itoa(i))
		assert.Nil(t, tree.traverse(key, false))
		assert.NotNil(t, tree.traverse(key, true))
		assert.NotNil(t, tree.traverse(key, false))
	}
}

func BenchmarkTraverse(b *testing.B) {
	buf := make([][]byte, b.N)
	for i := range buf {
		buf[i] = []byte(strconv.Itoa(i))
	}

	tree := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.traverse(buf[i], true)
	}
}
