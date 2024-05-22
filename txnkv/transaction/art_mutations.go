package transaction

import (
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/internal/unionstore"
)

type artMutations struct {
	storage *unionstore.MemDB

	// The format to put to the UserData of the handles:
	// MSB									                                                                              LSB
	// [12 bits: Op][1 bit: NeedConstraintCheckInPrewrite][1 bit: assertNotExist][1 bit: assertExist][1 bit: isPessimisticLock]
	handles []unionstore.MemKeyHandle
}

func newArtMutations(sizeHint int, storage *unionstore.MemDB) *artMutations {
	return &artMutations{
		handles: make([]unionstore.MemKeyHandle, 0, sizeHint),
		storage: storage,
	}
}

func (m *artMutations) Len() int {
	return len(m.handles)
}

func (m *artMutations) GetKey(i int) []byte {
	return m.storage.GetKeyByHandle(m.handles[i])
}

func (m *artMutations) GetKeys() [][]byte {
	ret := make([][]byte, m.Len())
	for i := range ret {
		ret[i] = m.GetKey(i)
	}
	return ret
}

func (m *artMutations) GetValue(i int) []byte {
	v, _ := m.storage.GetValueByHandle(m.handles[i])
	return v
}

func (m *artMutations) GetOp(i int) kvrpcpb.Op {
	return kvrpcpb.Op(m.handles[i].UserData >> 4)
}

func (m *artMutations) IsPessimisticLock(i int) bool {
	return m.handles[i].UserData&1 != 0
}

func (m *artMutations) IsAssertExists(i int) bool {
	return m.handles[i].UserData&(1<<1) != 0
}

func (m *artMutations) IsAssertNotExist(i int) bool {
	return m.handles[i].UserData&(1<<2) != 0
}

func (m *artMutations) NeedConstraintCheckInPrewrite(i int) bool {
	return m.handles[i].UserData&(1<<3) != 0
}

func (m *artMutations) Slice(from, to int) CommitterMutations {
	return &artMutations{
		handles: m.handles[from:to],
		storage: m.storage,
	}
}

func (m *artMutations) Push(op kvrpcpb.Op, isPessimisticLock, assertExist, assertNotExist, NeedConstraintCheckInPrewrite bool,
	handle unionstore.MemKeyHandle) {
	// See comments of `m.handles` field about the format of the user data `aux`.
	aux := uint16(op) << 4
	if isPessimisticLock {
		aux |= 1
	}
	if assertExist {
		aux |= 1 << 1
	}
	if assertNotExist {
		aux |= 1 << 2
	}
	if NeedConstraintCheckInPrewrite {
		aux |= 1 << 3
	}
	handle.UserData = aux
	m.handles = append(m.handles, handle)
}
