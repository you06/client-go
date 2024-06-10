package art

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestNodeSize(t *testing.T) {
	var (
		n4   node4
		n16  node16
		n48  node48
		n256 node256
	)

	require.Equal(t, int(unsafe.Sizeof(n4)), node4size)
	require.Equal(t, int(unsafe.Sizeof(n16)), node16size)
	require.Equal(t, int(unsafe.Sizeof(n48)), node48size)
	require.Equal(t, int(unsafe.Sizeof(n256)), node256size)
}

func TestAllocSize(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	require.Equal(t, int(allocator.node4Allocator.fixedSize), node4size)
	require.Equal(t, int(allocator.node16Allocator.fixedSize), node16size)
	require.Equal(t, int(allocator.node48Allocator.fixedSize), node48size)
	require.Equal(t, int(allocator.node256Allocator.fixedSize), node256size)

	addr, data := allocator.node4Allocator.alloc()
	require.False(t, addr.isNull())
	require.Len(t, data, node4size)
	addr, data = allocator.node16Allocator.alloc()
	require.False(t, addr.isNull())
	require.Len(t, data, node16size)
	addr, data = allocator.node48Allocator.alloc()
	require.False(t, addr.isNull())
	require.Len(t, data, node48size)
	addr, data = allocator.node256Allocator.alloc()
	require.False(t, addr.isNull())
	require.Len(t, data, node256size)
}

func TestAllocNode(t *testing.T) {
	var allocator artAllocator
	allocator.init()
	cnt := 10_000

	// alloc node4
	n4s := make([]*node4, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n4 := allocator.allocNode4()
		require.False(t, addr.isNull())
		require.NotNil(t, n4)
		n4.nodeNum = uint8(i % 4)
		n4s = append(n4s, n4)
	}
	for i, n4 := range n4s {
		require.Equal(t, uint8(i%4), n4.nodeNum)
	}

	// alloc node16
	n16s := make([]*node16, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n16 := allocator.allocNode16()
		require.False(t, addr.isNull())
		require.NotNil(t, n16)
		n16.nodeNum = uint8(i % 16)
		n16s = append(n16s, n16)
	}
	for i, n16 := range n16s {
		require.Equal(t, uint8(i%16), n16.nodeNum)
	}

	// alloc node48
	n48s := make([]*node48, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n48 := allocator.allocNode48()
		require.False(t, addr.isNull())
		require.NotNil(t, n48)
		n48.nodeNum = uint8(i % 48)
		n48s = append(n48s, n48)
	}
	for i, n48 := range n48s {
		require.Equal(t, uint8(i%48), n48.nodeNum)
	}

	// alloc node256
	n256s := make([]*node256, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n256 := allocator.allocNode256()
		require.False(t, addr.isNull())
		require.NotNil(t, n256)
		n256.nodeNum = uint8(i % 256)
		n256s = append(n256s, n256)
	}
	for i, n256 := range n256s {
		require.Equal(t, uint8(i%256), n256.nodeNum)
	}
}
