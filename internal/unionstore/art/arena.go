package art

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/tikv/client-go/v2/internal/logutil"
	"go.uber.org/zap"
)

const (
	alignMask = 1<<32 - 8 // 29 bit 1 and 3 bit 0.

	nullBlockOffset = math.MaxUint32
	maxBlockSize    = 128 << 20
	initBlockSize   = 4 * 1024
)

var (
	nullAddr = nodeAddr{math.MaxUint32, math.MaxUint32}
	endian   = binary.LittleEndian
)

type nodeAddr struct {
	idx uint32
	off uint32
}

func (addr nodeAddr) isNull() bool {
	if addr == nullAddr {
		return true
	}
	if addr.idx == math.MaxUint32 || addr.off == math.MaxUint32 {
		// defensive programming, the code should never run to here.
		// it always means something wrong... (maybe caused by data race?)
		// because we never set part of idx/off to math.MaxUint64
		logutil.BgLogger().Warn("Invalid nodeAddr", zap.Uint32("idx", addr.idx), zap.Uint32("off", addr.off))
		return true
	}
	return false
}

// store and load is used by vlog, due to pointer in vlog is not aligned.
func (addr nodeAddr) store(dst []byte) {
	endian.PutUint32(dst, addr.idx)
	endian.PutUint32(dst[4:], addr.off)
}

func (addr *nodeAddr) load(src []byte) {
	addr.idx = endian.Uint32(src)
	addr.off = endian.Uint32(src[4:])
}

type memArenaBlock struct {
	buf    []byte
	length int
}

type memArena struct {
	initBlockSize int
	blockSize     int
	blocks        []memArenaBlock
	// the total size of all blocks, also the approximate memory footprint of the arena.
	capacity uint64
}

// fixedSizeArena is a fixed size arena allocator.
// because the size of each type of node is fixed, the discarded nodes can be reused.
// reusing blocks reduces the memory pieces.
type fixedSizeArena struct {
	memArena
	fixedSize uint32
	freeNodes []nodeAddr
}

type vlogArena struct {
	memArena
}

type leafArena struct {
	memArena
}

type artAllocator struct {
	vlogAllocator    vlogArena
	node4Allocator   fixedSizeArena
	node16Allocator  fixedSizeArena
	node48Allocator  fixedSizeArena
	node256Allocator fixedSizeArena
	leafAllocator    leafArena
	// the total size of all blocks, also the approximate memory footprint of the arena.
	capacity uint64
}

func (allocator *artAllocator) init() {
	allocator.node4Allocator.fixedSize = uint32(node4size)
	allocator.node4Allocator.initBlockSize = node4size * 16
	allocator.node16Allocator.fixedSize = uint32(node16size)
	allocator.node16Allocator.initBlockSize = node16size * 8
	allocator.node48Allocator.fixedSize = uint32(node48size)
	allocator.node48Allocator.initBlockSize = node48size * 4
	allocator.node256Allocator.fixedSize = uint32(node256size)
	allocator.node256Allocator.initBlockSize = node256size * 2
	allocator.leafAllocator.initBlockSize = initBlockSize
	allocator.vlogAllocator.initBlockSize = initBlockSize
}

func (f *artAllocator) allocNode4() (nodeAddr, *node4) {
	addr, data := f.node4Allocator.alloc()
	n4 := (*node4)(unsafe.Pointer(&data[0]))
	n4.init()
	return addr, n4
}

func (f *artAllocator) freeNode4(addr nodeAddr) {
	f.node4Allocator.free(addr)
}

func (f *artAllocator) getNode4(addr nodeAddr) *node4 {
	if addr.isNull() {
		return nil
	}
	data := f.node4Allocator.getData(addr)
	return (*node4)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocNode16() (nodeAddr, *node16) {
	addr, data := f.node16Allocator.alloc()
	if len(data) != node16size {
		panic("alloc node16 failed")
	}
	n16 := (*node16)(unsafe.Pointer(&data[0]))
	n16.init()
	return addr, n16
}

func (f *artAllocator) freeNode16(addr nodeAddr) {
	f.node16Allocator.free(addr)
}

func (f *artAllocator) getNode16(addr nodeAddr) *node16 {
	if addr.isNull() {
		return nil
	}
	data := f.node16Allocator.getData(addr)
	return (*node16)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocNode48() (nodeAddr, *node48) {
	addr, data := f.node48Allocator.alloc()
	n48 := (*node48)(unsafe.Pointer(&data[0]))
	n48.init()
	return addr, n48
}

func (f *artAllocator) freeNode48(addr nodeAddr) {
	f.node48Allocator.free(addr)
}

func (f *artAllocator) getNode48(addr nodeAddr) *node48 {
	if addr.isNull() {
		return nil
	}
	data := f.node48Allocator.getData(addr)
	return (*node48)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocNode256() (nodeAddr, *node256) {
	addr, data := f.node256Allocator.alloc()
	n256 := (*node256)(unsafe.Pointer(&data[0]))
	n256.init()
	return addr, n256
}

func (f *artAllocator) freeNode256(addr nodeAddr) {
	f.node256Allocator.free(addr)
}

func (f *artAllocator) getNode256(addr nodeAddr) *node256 {
	if addr.isNull() {
		return nil
	}
	data := f.node256Allocator.getData(addr)
	return (*node256)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocLeaf(key Key) (nodeAddr, *leaf) {
	size := leafSize + len(key)
	addr, data := f.leafAllocator.alloc(size, true)
	lf := (*leaf)(unsafe.Pointer(&data[0]))
	lf.klen = uint16(len(key))
	lf.flags = 0
	lf.vAddr = nullAddr
	copy(data[leafSize:], key)
	return addr, lf
}

func (f *artAllocator) getLeaf(addr nodeAddr) *leaf {
	if addr.isNull() {
		return nil
	}
	data := f.leafAllocator.getData(addr)
	return (*leaf)(unsafe.Pointer(&data[0]))
}

// fixedSizeArena get data of fixed size.
func (f *fixedSizeArena) getData(addr nodeAddr) []byte {
	return f.blocks[addr.idx].buf[addr.off : addr.off+f.fixedSize]
}

func (f *fixedSizeArena) alloc() (nodeAddr, []byte) {
	if len(f.freeNodes) > 0 {
		addr := f.freeNodes[len(f.freeNodes)-1]
		f.freeNodes = f.freeNodes[:len(f.freeNodes)-1]
		return addr, f.getData(addr)
	}
	addr, data := f.memArena.alloc(int(f.fixedSize), true)
	return addr, data
}

func (f *fixedSizeArena) free(addr nodeAddr) {
	f.freeNodes = append(f.freeNodes, addr)
}

// memArena get all the data, DO NOT access others data.
func (a *memArena) getData(addr nodeAddr) []byte {
	return a.blocks[addr.idx].buf[addr.off:]
}

func (a *memArena) alloc(size int, align bool) (nodeAddr, []byte) {
	if size > maxBlockSize {
		panic("alloc size is larger than max block size")
	}

	if len(a.blocks) == 0 {
		a.enlarge(size, a.initBlockSize)
	}

	addr, data := a.allocInLastBlock(size, align)
	if !addr.isNull() {
		return addr, data
	}

	a.enlarge(size, a.blockSize<<1)
	return a.allocInLastBlock(size, align)
}

func (a *memArena) enlarge(allocSize, blockSize int) {
	a.blockSize = blockSize
	for a.blockSize <= allocSize {
		a.blockSize <<= 1
	}
	// Size will never larger than maxBlockSize.
	if a.blockSize > maxBlockSize {
		a.blockSize = maxBlockSize
	}
	a.blocks = append(a.blocks, memArenaBlock{
		buf: make([]byte, a.blockSize),
	})
	a.capacity += uint64(a.blockSize)
	// We shall not call a.onMemChange() here, since it will make the latest block empty, which breaks a precondition
	// for some operations (e.g. revertToCheckpoint)
}

func (a *memArena) allocInLastBlock(size int, align bool) (nodeAddr, []byte) {
	idx := len(a.blocks) - 1
	offset, data := a.blocks[idx].alloc(size, align)
	if offset == nullBlockOffset {
		return nullAddr, nil
	}
	return nodeAddr{uint32(idx), offset}, data
}

func (a *memArenaBlock) alloc(size int, align bool) (uint32, []byte) {
	offset := a.length
	if align {
		// We must align the allocated address for node
		// to make runtime.checkptrAlignment happy.
		offset = (a.length + 7) & alignMask
	}
	newLen := offset + size
	if newLen > len(a.buf) {
		return nullBlockOffset, nil
	}
	a.length = newLen
	return uint32(offset), a.buf[offset:newLen]
}

const memdbVlogHdrSize = 8 + 8 + 4

type vlogHdr struct {
	nodeAddr nodeAddr
	oldValue nodeAddr
	valueLen uint32
}

func (hdr *vlogHdr) store(dst []byte) {
	cursor := 0
	endian.PutUint32(dst[cursor:], hdr.valueLen)
	cursor += 4
	hdr.oldValue.store(dst[cursor:])
	cursor += 8
	hdr.nodeAddr.store(dst[cursor:])
}

func (hdr *vlogHdr) load(src []byte) {
	cursor := 0
	hdr.valueLen = endian.Uint32(src[cursor:])
	cursor += 4
	hdr.oldValue.load(src[cursor:])
	cursor += 8
	hdr.nodeAddr.load(src[cursor:])
}

func (f *artAllocator) allocValue(leafAddr nodeAddr, oldAddr nodeAddr, value []byte) nodeAddr {
	addr, data := f.vlogAllocator.alloc(memdbVlogHdrSize+len(value), true)
	copy(data[memdbVlogHdrSize:], value)
	hdr := vlogHdr{leafAddr, oldAddr, uint32(len(value))}
	hdr.store(data[:memdbVlogHdrSize])
	return addr
}

func (f *artAllocator) getValue(valAddr nodeAddr) []byte {
	data := f.vlogAllocator.getData(valAddr)
	var hdr vlogHdr
	hdr.load(data[:memdbVlogHdrSize])
	return data[memdbVlogHdrSize : memdbVlogHdrSize+hdr.valueLen]
}

func (f *artAllocator) trySwapValue(valAddr nodeAddr, val []byte) bool {
	if valAddr.isNull() {
		return false
	}
	data := f.vlogAllocator.getData(valAddr)
	var hdr vlogHdr
	hdr.load(data[:memdbVlogHdrSize])
	if int(hdr.valueLen) < len(val) {
		return false
	}
	copy(data[memdbVlogHdrSize:], val)
	hdr.valueLen = uint32(len(val))
	hdr.store(data[:memdbVlogHdrSize])
	return true
}
