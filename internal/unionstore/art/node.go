package art

type nodeKind uint16

const (
	typeNode4   nodeKind = 1
	typeNode16  nodeKind = 2
	typeNode48  nodeKind = 3
	typeNode256 nodeKind = 4
	typeLeaf    nodeKind = 5
)

const (
	maxPrefixLen = 10
	node4cap     = 4
	node16cap    = 16
	node48cap    = 48
	node256cap   = 256
)

const (
	node4size   = 60
	node16size  = 168
	node48size  = 696
	node256size = 2068
)

type artNode struct {
	kind nodeKind
	addr nodeAddr
}

type node struct {
	nodeNum     uint8
	prefixLen   uint8
	prefix      [maxPrefixLen]byte
	inplaceLeaf nodeAddr
}

type node4 struct {
	node
	present  uint8 // bitmap, only lower 4-bit is used
	keys     [node4cap]byte
	children [node4cap]nodeAddr
}

type node16 struct {
	node
	present  uint16 // bitmap
	keys     [node16cap]byte
	children [node16cap]nodeAddr
}

type node48 struct {
	node
	present  [4]uint64
	keys     [node256cap]uint8 // map byte to index
	children [node48cap]nodeAddr
}

type node256 struct {
	node
	children [node256cap]nodeAddr
}
