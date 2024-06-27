package art

import (
	"github.com/pkg/errors"
)

type ArtIterator struct {
	tree     *Art
	start    []byte
	end      []byte
	nodes    []artNode // node stack
	idxes    []int     // index stack
	currLeaf *leaf
	//reverse      bool
	//includeFlags bool
}

func (t *Art) Iter(k []byte, upperBound []byte) (*ArtIterator, error) {
	i := &ArtIterator{
		tree:  t,
		start: k,
		end:   upperBound,
		nodes: make([]artNode, 0, 4),
		idxes: make([]int, 0, 4),
	}
	i.init()
	if err := i.Next(); err != nil {
		return nil, err
	}
	return i, nil
}

func (it *ArtIterator) init() {
	if it.tree.root.addr.isNull() {
		return
	}
	curr := it.tree.root
	it.nodes = append(it.nodes, curr)
	it.idxes = append(it.idxes, -1)
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

func (it *ArtIterator) Value() []byte {
	return it.tree.getValue(it.currLeaf)
}

func (it *ArtIterator) Next() error {
	if len(it.nodes) == 0 {
		// iterate is finished
		return errors.New("Art: iterator is finished")
	}
	depth := len(it.nodes) - 1
	if depth == 1 {
		a := 1
		_ = a
	}
	curr := it.nodes[depth]
	idx := it.idxes[depth]
	switch curr.kind {
	case typeNode4:
		n4 := it.tree.allocator.getNode4(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n4.inplaceLeaf.addr != nullAddr {
				it.currLeaf = it.tree.allocator.getLeaf(n4.inplaceLeaf.addr)
				return nil
			}
		}
		for ; idx < int(n4.nodeNum); idx++ {
			if n4.children[idx].addr != nullAddr {
				it.idxes[depth] = idx + 1
				if n4.children[idx].kind == typeLeaf {
					it.currLeaf = it.tree.allocator.getLeaf(n4.children[idx].addr)
					return nil
				}
				it.nodes = append(it.nodes, n4.children[idx])
				it.idxes = append(it.idxes, -1)
				return it.Next()
			}
		}
	case typeNode16:
		n16 := it.tree.allocator.getNode16(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n16.inplaceLeaf.addr != nullAddr {
				it.currLeaf = it.tree.allocator.getLeaf(n16.inplaceLeaf.addr)
				return nil
			}
		}
		for ; idx < int(n16.nodeNum); idx++ {
			if n16.children[idx].addr != nullAddr {
				it.idxes[depth] = idx + 1
				if n16.children[idx].kind == typeLeaf {
					it.currLeaf = it.tree.allocator.getLeaf(n16.children[idx].addr)
					return nil
				}
				it.nodes = append(it.nodes, n16.children[idx])
				it.idxes = append(it.idxes, -1)
				return it.Next()
			}
		}
	case typeNode48:
		n48 := it.tree.allocator.getNode48(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n48.inplaceLeaf.addr != nullAddr {
				it.currLeaf = it.tree.allocator.getLeaf(n48.inplaceLeaf.addr)
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
				it.currLeaf = it.tree.allocator.getLeaf(child.addr)
				return nil
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, -1)
			return it.Next()
		}
	case typeNode256:
		n256 := it.tree.allocator.getNode256(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n256.inplaceLeaf.addr != nullAddr {
				it.currLeaf = it.tree.allocator.getLeaf(n256.inplaceLeaf.addr)
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
				it.currLeaf = it.tree.allocator.getLeaf(child.addr)
				return nil
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, -1)
			return it.Next()
		}
	}
	it.nodes = it.nodes[:depth]
	it.idxes = it.idxes[:depth]
	if depth == 0 {
		return nil
	}
	return it.Next()
}

func (it *ArtIterator) Close() {
	it.currLeaf = nil
	it.nodes = it.nodes[:0]
	it.idxes = it.idxes[:0]
}
