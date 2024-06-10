package art

import (
	"github.com/stretchr/testify/require"
	"testing"
	"unsafe"
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
