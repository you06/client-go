package art

import (
	"sync"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

var tombstone = []byte{}

type Art struct {
	sync.RWMutex
	skipMutex bool
	allocator artAllocator
	root      artNode
}

func New() *Art {
	t := &Art{
		root: nullArtNode,
	}
	t.allocator.init()
	return t
}

func (t *Art) Set(key, value []byte) error {
	return t.set(key, value, nil)
}

// SetWithFlags put key-value into the last active staging buffer with the given KeyFlags.
func (t *Art) SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return t.set(key, value, ops)
}

func (t *Art) Delete(key []byte) error {
	return t.set(key, tombstone, nil)
}

func (t *Art) Get(key []byte) ([]byte, error) {
	_, leaf := t.search(key)
	if leaf == nil || leaf.vAddr.isNull() {
		return nil, tikverr.ErrNotExist
	}
	return t.getValue(leaf), nil
}

// GetFlags returns the latest flags associated with key.
func (t *Art) GetFlags(key []byte) (kv.KeyFlags, error) {
	_, leaf := t.search(key)
	if leaf == nil {
		return 0, tikverr.ErrNotExist
	}
	return kv.KeyFlags(leaf.flags), nil
}

func (t *Art) set(key Key, value []byte, ops []kv.FlagsOp) error {
	if !t.skipMutex {
		t.Lock()
		defer t.Unlock()
	}
	addr, leaf := t.traverse(key, true)
	t.setValue(addr, leaf, value, ops)
	return nil
}

// traverse returns the node address of the key.
// if insert is true, it will insert the key if not exists, unless nullAddr is returned.
func (t *Art) traverse(key Key, insert bool) (nodeAddr, *leaf) {
	// lazy init root node and allocator.
	// this saves memory for read only txns.
	if t.root.addr.isNull() {
		if !insert {
			return nullAddr, nil
		}
		addr, _ := t.allocator.allocNode4()
		t.root = artNode{kind: typeNode4, addr: addr}
	}

	depth := uint32(0)
	prev := nullArtNode
	current := t.root
	for {
		prevDepth := int(depth - 1)
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
			an.addChild(&t.allocator, l1Key.charAt(int(depth)), !l1Key.valid(int(depth)), current)
			an.addChild(&t.allocator, l2Key.charAt(int(depth)), !l2Key.valid(int(depth)), newLeafAddr)
			if prev == nullArtNode {
				t.root = an
			} else {
				prev.swapChild(&t.allocator, key.charAt(prevDepth), an)
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
					grown := current.addChild(&t.allocator, key.charAt(int(depth)), !key.valid(int(depth)), newLeaf)
					if grown {
						if prev == nullArtNode {
							t.root = current
						} else {
							prev.swapChild(&t.allocator, key.charAt(prevDepth), current)
						}
					}
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
			copy(newN4.prefix[:], key[depth:depth+mismatchIdx])

			// move the current node as the children of the new node.
			if node.prefixLen <= maxPrefixLen {
				nodeKey := node.prefix[mismatchIdx]
				node.prefixLen -= uint8(mismatchIdx + 1)
				copy(node.prefix[:], node.prefix[mismatchIdx+1:])
				newArtNode.addChild(&t.allocator, nodeKey, false, current)
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
				prev.swapChild(&t.allocator, key.charAt(prevDepth), newArtNode)
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
					prev.swapChild(&t.allocator, key.charAt(prevDepth), current)
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

func (t *Art) search(key Key) (nodeAddr, *leaf) {
	current := t.root
	if current == nullArtNode {
		return nullAddr, nil
	}
	depth := uint32(0)
	for {
		if current.isLeaf() {
			lf := current.leaf(&t.allocator)
			if lf.match(key) {
				return current.addr, lf
			}
			return nullAddr, nil
		}

		node := current.node(&t.allocator)
		if node.prefixLen > 0 {
			prefixLen := node.match(key, depth)
			if int(prefixLen) != min(int(node.prefixLen), maxPrefixLen) {
				return nullAddr, nil
			}
			depth += uint32(node.prefixLen)
		}

		current = current.findChild(&t.allocator, key.charAt(int(depth)), key.valid(int(depth)))
		if current.addr == nullAddr {
			return nullAddr, nil
		}
		depth++
	}
	return nullAddr, nil
}

func (t *Art) newNode4() (artNode, *node4) {
	addr, n4 := t.allocator.allocNode4()
	return artNode{kind: typeNode4, addr: addr}, n4
}

func (t *Art) newLeaf(key Key) (artNode, *leaf) {
	addr, lf := t.allocator.allocLeaf(key)
	return artNode{kind: typeLeaf, addr: addr}, lf
}

func (t *Art) longestCommonPrefix(l1Key, l2Key Key, depth uint32) uint32 {
	idx, limit := depth, min(uint32(len(l1Key)), uint32(len(l2Key)))
	for ; idx < limit; idx++ {
		if l1Key[idx] != l2Key[idx] {
			break
		}
	}

	return idx - depth
}

func (t *Art) setValue(addr nodeAddr, l *leaf, value []byte, ops []kv.FlagsOp) {
	var flags kv.KeyFlags
	if value != nil {
		flags = kv.ApplyFlagsOps(l.getKeyFlags(), append([]kv.FlagsOp{kv.DelNeedConstraintCheckInPrewrite}, ops...)...)
	} else {
		// an UpdateFlag operation, do not delete the NeedConstraintCheckInPrewrite flag.
		flags = kv.ApplyFlagsOps(l.getKeyFlags(), ops...)
	}
	l.flags = uint16(flags)
	if t.allocator.trySwapValue(l.vAddr, value) {
		return
	}
	vAddr := t.allocator.allocValue(addr, nullAddr, value)
	l.vAddr = vAddr
}

func (t *Art) getValue(l *leaf) []byte {
	if l.vAddr.isNull() {
		return nil
	}
	return t.allocator.getValue(l.vAddr)
}
