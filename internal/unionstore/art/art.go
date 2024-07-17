package art

import (
	"fmt"
	"sync"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

var tombstone = []byte{}

type Art struct {
	sync.RWMutex
	skipMutex   bool
	allocator   artAllocator
	root        artNode
	stages      []ARTCheckpoint
	vlogInvalid bool
	dirty       bool
	len         int
	size        int
}

func New() *Art {
	t := &Art{
		root:   nullArtNode,
		stages: make([]ARTCheckpoint, 0, 2),
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

func (t *Art) UpdateFlags(key []byte, ops ...kv.FlagsOp) {
	_ = t.set(key, nil, ops)
}

func (t *Art) Delete(key []byte) error {
	return t.set(key, tombstone, nil)
}

func (t *Art) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	return t.set(key, tombstone, ops)
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
	t.len++
	t.size += len(key) + len(value)
	addr, leaf := t.recursiveInsert(key)
	t.setValue(addr, leaf, value, ops)
	return nil
}

// recursiveInsert returns the node address of the key.
// if insert is true, it will insert the key if not exists, unless nullAddr is returned.
func (t *Art) recursiveInsert(key Key) (nodeAddr, *leaf) {
	// lazy init root node and allocator.
	// this saves memory for read only txns.
	if t.root.addr.isNull() {
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
				leafArtNode := minimum(&t.allocator, current)
				leaf := leafArtNode.leaf(&t.allocator)
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
			if prefixLen < uint32(node.prefixLen) {
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
	if t.trySwapValue(l.vAddr, value) {
		return
	}
	vAddr := t.allocator.allocValue(addr, l.vAddr, value)
	l.vAddr = vAddr
}

func (t *Art) trySwapValue(addr nodeAddr, value []byte) bool {
	if addr.isNull() {
		return false
	}
	if len(t.stages) == 0 {
		return true
	}
	cp := t.stages[len(t.stages)-1]
	if !t.canSwapValue(&cp, addr) {
		return false
	}
	oldVal := t.allocator.getValue(addr)
	if len(oldVal) > 0 && len(oldVal) == len(value) {
		copy(oldVal, value)
	}
	return true
}

func (t *Art) canSwapValue(cp *ARTCheckpoint, addr nodeAddr) bool {
	if cp == nil {
		return true
	}
	if int(addr.idx) >= cp.blocks {
		return true
	}
	if int(addr.idx) == cp.blocks-1 && int(addr.off) > cp.offsetInBlock {
		return true
	}
	return false
}

func (t *Art) getValue(l *leaf) []byte {
	if l.vAddr.isNull() {
		return nil
	}
	return t.allocator.getValue(l.vAddr)
}

func (t *Art) Empty() bool {
	return t.root.addr.isNull()
}

// Mem returns the memory usage of MemBuffer.
func (t *Art) Mem() uint64 {
	return t.allocator.vlogAllocator.capacity + t.allocator.nodeAllocator.capacity
}

// Len returns the count of entries in the MemBuffer.
func (t *Art) Len() int {
	return t.len
}

// Size returns the size of the MemBuffer.
func (t *Art) Size() int {
	return t.size
}

// ARTCheckpoint is the checkpoint of memory DB.
type ARTCheckpoint struct {
	blockSize     int
	blocks        int
	offsetInBlock int
}

func (cp *ARTCheckpoint) isSamePosition(other *ARTCheckpoint) bool {
	return cp.blocks == other.blocks && cp.offsetInBlock == other.offsetInBlock
}

func (t *Art) checkpoint() ARTCheckpoint {
	snap := ARTCheckpoint{
		blockSize: t.allocator.vlogAllocator.blockSize,
		blocks:    len(t.allocator.vlogAllocator.blocks),
	}
	if snap.blocks > 0 {
		snap.offsetInBlock = t.allocator.vlogAllocator.blocks[snap.blocks-1].length
	}
	return snap
}

func (t *Art) Stages() []ARTCheckpoint {
	return t.stages
}

func (t *Art) Staging() int {
	if !t.skipMutex {
		t.Lock()
		defer t.Unlock()
	}
	t.stages = append(t.stages, t.checkpoint())
	return len(t.stages)
}

func (t *Art) Release(h int) {
	if !t.skipMutex {
		t.Lock()
		defer t.Unlock()
	}
	if h != len(t.stages) {
		panic("cannot release staging buffer")
	}
	if h == 1 {
		tail := t.checkpoint()
		if !t.stages[0].isSamePosition(&tail) {
			t.dirty = true
		}
	}
	t.stages = t.stages[:h-1]
}

func (t *Art) Cleanup(h int) {
	if !t.skipMutex {
		t.Lock()
		defer t.Unlock()
	}

	if h > len(t.stages) {
		return
	}
	if h < len(t.stages) {
		panic(fmt.Sprintf("cannot cleanup staging buffer, h=%v, len(db.stages)=%v", h, len(t.stages)))
	}

	cp := &t.stages[h-1]
	{
		curr := t.checkpoint()
		if !curr.isSamePosition(cp) {
			t.revertToCheckpoint(cp)
			t.truncate(cp)
		}
	}
	t.stages = t.stages[:h-1]
}

func (t *Art) revertToCheckpoint(cp *ARTCheckpoint) {
	cursor := t.checkpoint()
	for !cp.isSamePosition(&cursor) {
		hdrOff := cursor.offsetInBlock - memdbVlogHdrSize
		block := t.allocator.vlogAllocator.blocks[cursor.blocks-1].buf
		var hdr vlogHdr
		hdr.load(block[hdrOff:])
		lf := t.allocator.getLeaf(hdr.nodeAddr)
		lf.vAddr = hdr.oldValue
		t.size -= int(hdr.valueLen)
		if !hdr.oldValue.isNull() {
			t.size += len(t.allocator.getValue(hdr.oldValue))
		}
		t.moveBackCursor(&cursor, &hdr)
	}
}

func (t *Art) moveBackCursor(cursor *ARTCheckpoint, hdr *vlogHdr) {
	cursor.offsetInBlock -= (memdbVlogHdrSize + int(hdr.valueLen))
	if cursor.offsetInBlock == 0 {
		cursor.blocks--
		if cursor.blocks > 0 {
			cursor.offsetInBlock = t.allocator.vlogAllocator.blocks[cursor.blocks-1].length
		}
	}
}

func (t *Art) truncate(snap *ARTCheckpoint) {
	vlogAllocator := &t.allocator.vlogAllocator
	for i := snap.blocks; i < len(vlogAllocator.blocks); i++ {
		vlogAllocator.blocks[i] = memArenaBlock{}
	}
	vlogAllocator.blocks = vlogAllocator.blocks[:snap.blocks]
	if len(vlogAllocator.blocks) > 0 {
		vlogAllocator.blocks[len(vlogAllocator.blocks)-1].length = snap.offsetInBlock
	}
	vlogAllocator.blockSize = snap.blockSize
	// recalculate the capacity
	vlogAllocator.capacity = 0
	for _, block := range vlogAllocator.blocks {
		vlogAllocator.capacity += uint64(block.length)
	}
	// We shall not call a.onMemChange() here, since it may cause a panic and leave memdb in an inconsistent state
}

// Checkpoint returns a checkpoint of MemDB.
func (t *Art) Checkpoint() ARTCheckpoint {
	return t.checkpoint()
}

// RevertToCheckpoint reverts the MemDB to the checkpoint.
func (t *Art) RevertToCheckpoint(cp ARTCheckpoint) {
	t.revertToCheckpoint(&cp)
	t.truncate(&cp)
}

// DiscardValues releases the memory used by all values.
// NOTE: any operation need value will panic after this function.
func (t *Art) DiscardValues() {
	t.vlogInvalid = true
	t.allocator.vlogAllocator.reset()
}

// InspectStage used to inspect the value updates in the given stage.
func (t *Art) InspectStage(handle int, f func([]byte, kv.KeyFlags, []byte)) {
	idx := handle - 1
	tail := t.checkpoint()
	head := t.stages[idx]
	t.inspectKVInLog(&head, &tail, f)
}

func (t *Art) inspectKVInLog(head, tail *ARTCheckpoint, f func([]byte, kv.KeyFlags, []byte)) {
	cursor := *tail
	for !head.isSamePosition(&cursor) {
		cursorAddr := nodeAddr{idx: uint32(cursor.blocks - 1), off: uint32(cursor.offsetInBlock)}
		hdrOff := cursorAddr.off - memdbVlogHdrSize
		block := t.allocator.vlogAllocator.blocks[cursorAddr.idx].buf
		var hdr vlogHdr
		hdr.load(block[hdrOff:])
		lf := t.allocator.getLeaf(hdr.nodeAddr)

		// Skip older versions.
		if lf.vAddr == cursorAddr {
			value := block[hdrOff-hdr.valueLen : hdrOff]
			f(lf.getKey(), kv.KeyFlags(lf.flags), value)
		}
		t.moveBackCursor(&cursor, &hdr)
	}
}

// SelectValueHistory select the latest value which makes `predicate` returns true from the modification history.
func (t *Art) SelectValueHistory(key []byte, predicate func(value []byte) bool) ([]byte, error) {
	_, x := t.search(key)
	if x == nil {
		return nil, tikverr.ErrNotExist
	}
	if x.vAddr.isNull() {
		// A flags only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	result := t.selectValueHistory(x.vAddr, func(addr nodeAddr) bool {
		return predicate(t.allocator.getValue(addr))
	})
	if result.isNull() {
		return nil, nil
	}
	return t.allocator.getValue(result), nil
}

func (t *Art) selectValueHistory(addr nodeAddr, predicate func(nodeAddr) bool) nodeAddr {
	for !addr.isNull() {
		if predicate(addr) {
			return addr
		}
		var hdr vlogHdr
		hdr.load(t.allocator.vlogAllocator.blocks[addr.idx].buf[addr.off-memdbVlogHdrSize:])
		addr = hdr.oldValue
	}
	return nullAddr
}

func (t *Art) getSnapshotValue(addr nodeAddr, cp *ARTCheckpoint) ([]byte, bool) {
	result := t.selectValueHistory(addr, func(addr nodeAddr) bool {
		return !t.canSwapValue(cp, addr)
	})
	if result.isNull() {
		return nil, false
	}
	return t.allocator.getValue(result), true
}

func (t *Art) SetMemoryFootprintChangeHook(fn func(uint64)) {
	hook := func() {
		fn(t.allocator.nodeAllocator.capacity + t.allocator.vlogAllocator.capacity)
	}
	t.allocator.nodeAllocator.memChangeHook.Store(&hook)
	t.allocator.vlogAllocator.memChangeHook.Store(&hook)
}

// MemHookSet implements the MemBuffer interface.
func (t *Art) MemHookSet() bool {
	return t.allocator.nodeAllocator.memChangeHook.Load() != nil
}

// GetKeyByHandle returns key by handle.
func (t *Art) GetKeyByHandle(handle ArtMemKeyHandle) []byte {
	lf := t.allocator.getLeaf(handle.toAddr())
	return lf.getKey()
}

// GetValueByHandle returns value by handle.
func (t *Art) GetValueByHandle(handle ArtMemKeyHandle) ([]byte, bool) {
	if t.vlogInvalid {
		return nil, false
	}
	lf := t.allocator.getLeaf(handle.toAddr())
	if lf.vAddr.isNull() {
		return nil, false
	}
	return t.allocator.getValue(lf.vAddr), true
}
