package art

import (
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/kv"
)

type ArtIterator struct {
	tree     *Art
	reverse  bool
	valid    bool
	nodes    []artNode // node stack
	idxes    []int     // index stack
	endIdxes []int
	currLeaf *leaf
	currAddr nodeAddr
	//includeFlags bool
}

func (t *Art) Iter(lowerBound, upperBound []byte) (*ArtIterator, error) {
	i := &ArtIterator{
		tree:    t,
		reverse: false,
		valid:   true,
		nodes:   make([]artNode, 0, 4),
		idxes:   make([]int, 0, 4),
	}
	i.init(lowerBound, upperBound)
	if err := i.Next(); err != nil {
		return nil, err
	}
	return i, nil
}

func (it *ArtIterator) init(lowerBound, upperBound []byte) {
	if it.tree.root.addr.isNull() {
		it.valid = false
		return
	}
	if it.reverse {
		it.idxes, it.nodes = it.seek(upperBound, true)
		it.endIdxes, _ = it.seek(lowerBound, false)
	} else {
		it.idxes, it.nodes = it.seek(lowerBound, false)
		it.endIdxes, _ = it.seek(upperBound, true)
	}
	return
	/*
		for {
			if curr.addr.isNull() {
				return
			}
			if curr.kind == typeLeaf {
				return
			}
			it.nodes = append(it.nodes, curr)
			switch curr.kind {
			case typeNode4:
				n4 := it.tree.allocator.getNode4(curr.addr)
				if n4.nodeNum == 0 || n4.inplaceLeaf.addr != nullAddr {
					it.idxes = append(it.idxes, -1) // -1 stands for in-place node
					return
				}
				curr = n4.children[0]
				it.idxes = append(it.idxes, 0)
			case typeNode16:
				n16 := it.tree.allocator.getNode16(curr.addr)
				if n16.nodeNum == 0 || n16.inplaceLeaf.addr != nullAddr {
					it.idxes = append(it.idxes, -1) // -1 stands for in-place node
					return
				}
				curr = n16.children[0]
				it.idxes = append(it.idxes, 0)
			case typeNode48:
				n48 := it.tree.allocator.getNode48(curr.addr)
				if n48.nodeNum == 0 || n48.inplaceLeaf.addr != nullAddr {
					it.idxes = append(it.idxes, -1) // -1 stands for in-place node
					return
				}
				idx := n48.nextPresentIdx(0)
				it.idxes = append(it.idxes, idx)
				curr = n48.children[n48.keys[idx]]
			case typeNode256:
				n256 := it.tree.allocator.getNode256(curr.addr)
				if n256.nodeNum == 0 || n256.inplaceLeaf.addr != nullAddr {
					it.idxes = append(it.idxes, -1) // -1 stands for in-place node
					return
				}
				idx := n256.nextPresentIdx(0)
				it.idxes = append(it.idxes, idx)
				curr = n256.children[idx]
			}
		}
	*/
}

func (it *ArtIterator) Valid() bool { return len(it.nodes) > 0 }

func (it *ArtIterator) Key() []byte {
	return it.currLeaf.getKey()
}

func (it *ArtIterator) Flags() kv.KeyFlags {
	return kv.KeyFlags(it.currLeaf.flags)
}

func (it *ArtIterator) Value() []byte {
	return it.tree.getValue(it.currLeaf)
}

// seek the first leaf node that >= key
// if upper, seek the last leaf node that < key
func (it *ArtIterator) seek(key Key, upper bool) ([]int, []artNode) {
	curr := it.tree.root
	depth := uint32(0)
	idxes := make([]int, 0, 4)
	nodes := make([]artNode, 0, 4)
	for {
		if curr.isLeaf() {
			break
		}

		node := curr.node(&it.tree.allocator)
		if node.prefixLen > 0 {
			prefixLen := node.match(key, depth)
			if prefixLen < uint32(node.prefixLen) {
				diffIdx := prefixLen + 1
				less := len(key) < int(depth+diffIdx) || key[depth+diffIdx] < node.prefix[diffIdx]
				if less {
					if upper {
						idxes = append(idxes, int(node.nodeNum-1))
						nodes = append(nodes, curr)
					}
				} else {
					if !upper {
						idxes = append(idxes, -1)
						nodes = append(nodes, curr)
					}
				}
				return idxes, nodes
			}
			depth += uint32(node.prefixLen)
		}

		char := key.charAt(int(depth))
		next := curr.findChild(&it.tree.allocator, char, key.valid(int(depth)))
		nodes = append(nodes, curr)
		if curr.addr == nullAddr {
			var near int
			switch curr.kind {
			case typeNode4:
				n4 := curr.node4(&it.tree.allocator)
				var i int
				if upper {
					i = int(n4.nodeNum) - 1
					for ; i >= 0; i-- {
						if n4.keys[i] < char {
							break
						}
					}
				} else {
					i = 0
					for ; i < int(n4.nodeNum); i++ {
						if n4.keys[i] >= char {
							break
						}
					}
				}
				near = i
			case typeNode16:
				n16 := curr.node16(&it.tree.allocator)
				var i int
				if upper {
					i = int(n16.nodeNum) - 1
					for ; i >= 0; i-- {
						if n16.keys[i] < char {
							break
						}
					}
				} else {
					i = 0
					for ; i < int(n16.nodeNum); i++ {
						if n16.keys[i] >= char {
							break
						}
					}
				}
				near = i
			case typeNode48:
				n48 := curr.node48(&it.tree.allocator)
				if upper {
					near = n48.prevPresentIdx(int(char))
				} else {
					near = n48.nextPresentIdx(int(char))
				}
			case typeNode256:
				n256 := curr.node256(&it.tree.allocator)
				if upper {
					near = n256.prevPresentIdx(int(char))
				} else {
					near = n256.nextPresentIdx(int(char))
				}
			}
			idxes = append(idxes, near)
			return idxes, nodes
		}
		idxes = append(idxes, int(char))
		curr = next
		depth++
	}
	return idxes, nodes
}

func (it *ArtIterator) Next() error {
	if it.reverse {
		return it.prev(true)
	}
	return it.next(true)
}

func (it *ArtIterator) next(last bool) error {
	if len(it.nodes) == 0 {
		// iterate is finished
		return errors.New("Art: iterator is finished")
	}
	depth := len(it.nodes) - 1
	curr := it.nodes[depth]
	idx := it.idxes[depth]
	if last {
		if depth >= len(it.endIdxes) {
			it.valid = false
			return nil
		}
		last = idx == it.endIdxes[depth]
	}
	switch curr.kind {
	case typeNode4:
		n4 := it.tree.allocator.getNode4(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n4.inplaceLeaf.addr != nullAddr {
				it.setCurrLeaf(n4.inplaceLeaf.addr)
				return nil
			}
		}
		for ; idx < int(n4.nodeNum); idx++ {
			if n4.children[idx].addr != nullAddr {
				it.idxes[depth] = idx + 1
				if n4.children[idx].kind == typeLeaf {
					it.setCurrLeaf(n4.children[idx].addr)
					return nil
				}
				it.nodes = append(it.nodes, n4.children[idx])
				it.idxes = append(it.idxes, -1)
				return it.next(last)
			}
		}
	case typeNode16:
		n16 := it.tree.allocator.getNode16(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n16.inplaceLeaf.addr != nullAddr {
				it.setCurrLeaf(n16.inplaceLeaf.addr)
				return nil
			}
		}
		for ; idx < int(n16.nodeNum); idx++ {
			if n16.children[idx].addr != nullAddr {
				it.idxes[depth] = idx + 1
				if n16.children[idx].kind == typeLeaf {
					it.setCurrLeaf(n16.children[idx].addr)
					return nil
				}
				it.nodes = append(it.nodes, n16.children[idx])
				it.idxes = append(it.idxes, -1)
				return it.next(last)
			}
		}
	case typeNode48:
		n48 := it.tree.allocator.getNode48(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n48.inplaceLeaf.addr != nullAddr {
				it.setCurrLeaf(n48.inplaceLeaf.addr)
				return nil
			}
		} else if idx == 256 {
			break
		}
		idx = n48.nextPresentIdx(idx)
		if idx < 256 {
			it.idxes[depth] = idx + 1
			child := n48.children[n48.keys[idx]]
			if child.kind == typeLeaf {
				it.setCurrLeaf(child.addr)
				return nil
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, -1)
			return it.next(last)
		}
	case typeNode256:
		n256 := it.tree.allocator.getNode256(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n256.inplaceLeaf.addr != nullAddr {
				it.setCurrLeaf(n256.inplaceLeaf.addr)
				return nil
			}
		} else if idx == 256 {
			break
		}
		idx = n256.nextPresentIdx(idx)
		if idx < 256 {
			it.idxes[depth] = idx + 1
			child := n256.children[idx]
			if child.kind == typeLeaf {
				it.setCurrLeaf(child.addr)
				return nil
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, -1)
			return it.next(last)
		}
	}
	it.nodes = it.nodes[:depth]
	it.idxes = it.idxes[:depth]
	if depth == 0 {
		return nil
	}
	return it.next(last)
}

func (it *ArtIterator) prev(last bool) error {
	return nil
}

func (it *ArtIterator) setCurrLeaf(node nodeAddr) {
	it.currAddr = node
	it.currLeaf = it.tree.allocator.getLeaf(node)
}

func (it *ArtIterator) Close() {
	it.currLeaf = nil
	it.nodes = it.nodes[:0]
	it.idxes = it.idxes[:0]
}

type ArtMemKeyHandle struct {
	UserData uint16
	idx      uint16
	off      uint32
}

func (it *ArtIterator) Handle() ArtMemKeyHandle {
	return ArtMemKeyHandle{
		idx: uint16(it.currAddr.idx),
		off: it.currAddr.off,
	}
}

func (h ArtMemKeyHandle) toAddr() nodeAddr {
	return nodeAddr{idx: uint32(h.idx), off: h.off}
}
