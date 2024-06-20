package art

import (
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

var tombstone = []byte{}

type art struct {
	allocator artAllocator
	root      artNode
}

func New() *art {
	t := &art{
		root: nullArtNode,
	}
	t.allocator.init()
	return t
}

func (t *art) Set(key, value []byte) error {
	return t.set(key, value)
}

func (t *art) Delete(key []byte) error {
	return t.set(key, tombstone)
}

func (t *art) Get(key []byte) ([]byte, error) {
	_, leaf := t.traverse(key, false)
	if leaf == nil || leaf.vAddr.isNull() {
		return nil, tikverr.ErrNotExist
	}
	return t.getValue(leaf), nil
}

func (t *art) set(key Key, value []byte, ops []kv.FlagsOp) error {
	addr, leaf := t.traverse(key, true)
	t.setValue(addr, leaf, value, ops)
	return nil
}

// traverse returns the node address of the key.
// if insert is true, it will insert the key if not exists, unless nullAddr is returned.
func (t *art) traverse(key Key, insert bool) (nodeAddr, *leaf) {
	// lazy init root node and allocator.
	// this saves memory for read only txns.
	if t.root.addr.isNull() {
		if !insert {
			return nullAddr, nil
		}
		addr, _ := t.allocator.node4Allocator.alloc()
		t.root = artNode{kind: typeNode4, addr: addr}
	}

	depth := uint32(0)
	prev := nullArtNode
	current := t.root
	for {
		if current.isLeaf() {
			leaf1 := current.leaf(&t.allocator)
			if leaf1.match(key) {
				return current.addr, leaf1
			}
			if !insert {
				return nullAddr, nil
			}
			newLeafAddr, leaf2 := t.newLeaf(key)
			l1Key, l2Key := leaf1.getKey(), leaf2.getKey()
			lcp := t.longestCommonPrefix(l1Key, l2Key, depth)
			an, n4 := t.newNode4()
			n4.setPrefix(key[depth:], lcp)
			depth += lcp
			an.addChild(&t.allocator, l1Key.charAt(int(depth+lcp)), !l1Key.valid(int(depth+lcp)), current)
			an.addChild(&t.allocator, l2Key.charAt(int(depth+lcp)), !l2Key.valid(int(depth+lcp)), newLeafAddr)
			if prev == nullArtNode {
				t.root = an
			} else {
				prev.swapChild(&t.allocator, key.charAt(int(depth-1)), an)
			}
			return newLeafAddr.addr, leaf2
		}

		node := current.node(&t.allocator)
		if node.prefixLen > 0 {
			mismatchIdx := current.matchDeep(&t.allocator, key, depth)
			if mismatchIdx >= uint32(node.prefixLen) {
				// all the prefix match, go deeper.
				depth += uint32(node.prefixLen)
				next := current.findChild(&t.allocator, key.charAt(int(depth)), key.valid(int(depth)))

				if next == nullArtNode {
					if !insert {
						return nullAddr, nil
					}
					newLeaf, lf := t.newLeaf(key)
					current.addChild(&t.allocator, key.charAt(int(depth)), !key.valid(int(depth)), newLeaf)
					return newLeaf.addr, lf
				}
				prev = current
				current = next
				depth++
				continue
			}
			// instead, we split the node into different prefixes.
			newArtNode, newN4 := t.newNode4()
			newN4.prefixLen = uint8(mismatchIdx)
			copy(newN4.prefix[:], key[depth:depth+uint32(mismatchIdx)])

			// move the current node as the children of the new node.
			if node.prefixLen <= maxPrefixLen {
				node.prefixLen -= uint8(mismatchIdx + 1)
				copy(node.prefix[:], node.prefix[mismatchIdx:])
				newArtNode.addChild(&t.allocator, key.charAt(int(depth+mismatchIdx)), !key.valid(int(depth+mismatchIdx)), current)
			} else {
				node.prefixLen -= uint8(mismatchIdx + 1)
				leaf := current.minimum(&t.allocator)
				leafKey := leaf.getKey()
				newArtNode.addChild(&t.allocator, leafKey.charAt(int(depth+mismatchIdx)), !leafKey.valid(int(depth)), current)
			}

			// insert the leaf into new node
			newLeafAddr, newLeaf := t.newLeaf(key)
			newArtNode.addChild(&t.allocator, key.charAt(int(depth+mismatchIdx)), !key.valid(int(depth+mismatchIdx)), newLeafAddr)
			if prev == nullArtNode {
				t.root = newArtNode
			} else {
				prev.swapChild(&t.allocator, key.charAt(int(depth-1)), newArtNode)
			}
			return newLeafAddr.addr, newLeaf
		}
		// next
		valid := key.valid(int(depth))
		next := current.findChild(&t.allocator, key.charAt(int(depth)), valid)
		if next == nullArtNode {
			if !insert {
				return nullAddr, nil
			}
			newLeaf, lf := t.newLeaf(key)
			if current.addChild(&t.allocator, key.charAt(int(depth)), !key.valid(int(depth)), newLeaf) {
				if prev == nullArtNode {
					t.root = current
				} else {
					prev.swapChild(&t.allocator, key.charAt(int(depth-1)), current)
				}
			}
			return newLeaf.addr, lf
		}
		if !valid && next.kind == typeLeaf {
			return next.addr, next.leaf(&t.allocator)
		}
		prev = current
		current = next
		depth++
		continue
	}
}

func (t *art) newNode4() (artNode, *node4) {
	addr, n4 := t.allocator.allocNode4()
	return artNode{kind: typeNode4, addr: addr}, n4
}

func (t *art) newLeaf(key Key) (artNode, *leaf) {
	addr, lf := t.allocator.allocLeaf(key)
	return artNode{kind: typeLeaf, addr: addr}, lf
}

func (t *art) longestCommonPrefix(l1Key, l2Key Key, depth uint32) uint32 {
	idx, limit := depth, min(uint32(len(l1Key)), uint32(len(l2Key)))
	for ; idx < limit; idx++ {
		if l1Key[idx] != l2Key[idx] {
			break
		}
	}

	return idx - depth
}

func (t *art) setValue(addr nodeAddr, l *leaf, value []byte, ops []kv.FlagsOp) {
	vAddr := t.allocator.allocValue(addr, nullAddr, value, ops)
	l.vAddr = vAddr
}

func (t *art) getValue(l *leaf) []byte {
	if l.vAddr.isNull() {
		return nil
	}
	return t.allocator.getValue(l.vAddr)
}
