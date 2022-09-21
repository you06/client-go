package oracles

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

type MockClock []uint64

func (m MockClock) next() (uint64, MockClock) {
	return m[0], m[1:]
}

func mockClock(times []uint64) ClockFn {
	m := MockClock(times)
	return func() uint64 {
		if len(m) == 0 {
			panic("not enough mock time")
		}
		var t uint64
		t, m = m.next()
		return t
	}
}

func fixedPhysicalTime(physical uint64) ClockFn {
	return func() uint64 {
		return physical
	}
}

func TestMockClock(t *testing.T) {
	times := []uint64{10, 20, 30, 40}
	clock := mockClock(times)
	for _, now := range times {
		assert.Equal(t, clock(), now)
	}
}

func TestFixedPhysicalTime(t *testing.T) {
	hlcClock := NewHLCClock(fixedPhysicalTime(123))
	for i := 0; i < 10; i++ {
		now, err := hlcClock.GetTimestamp(context.TODO(), nil)
		assert.Nil(t, err)
		assert.Equal(t, uint64(123<<18+i), now)
	}
}

func TestFlowPhysicalTime(t *testing.T) {
	physicalTimeCounts := []int{1, 2, 3, 4, 5}
	physicalTimes := []uint64{}
	for p, c := range physicalTimeCounts {
		for i := 0; i < c; i++ {
			physicalTimes = append(physicalTimes, uint64(p+1))
		}
	}
	hlcClock := NewHLCClock(mockClock(physicalTimes))
	for p, c := range physicalTimeCounts {
		for i := 0; i < c; i++ {
			now, err := hlcClock.GetTimestamp(context.TODO(), nil)
			assert.Nil(t, err)
			assert.Equal(t, uint64((p+1)<<18+i), now)
		}
	}
}

func TestPushPhysicalTime(t *testing.T) {
	physicalTimeCounts := []int{1, 2, 3, 4, 5}
	physicalTimes := []uint64{}
	for p, c := range physicalTimeCounts {
		for i := 0; i < c; i++ {
			physicalTimes = append(physicalTimes, uint64(p+1))
		}
	}
	hlcClock := NewHLCClock(mockClock(physicalTimes))
	{
		now, err := hlcClock.GetTimestamp(context.TODO(), nil)
		assert.Nil(t, err)
		assert.Equal(t, uint64(1<<18), now)
	}
	// push time to (3, 0)
	hlcClock.(*HLCClock).OnMsg(uint64(3 << 18))
	// allocate time from (3, 2) to (3, 5), note that (3, 1) will be skipped.
	// the physical clock is called once in OnMsg function, so only 4 clock left.
	for i := 0; i < 4; i++ {
		now, err := hlcClock.GetTimestamp(context.TODO(), nil)
		assert.Nil(t, err)
		assert.Equal(t, uint64(3<<18+i+2), now)
	}
	// physical time grows.
	for i := 0; i < 4; i++ {
		now, err := hlcClock.GetTimestamp(context.TODO(), nil)
		assert.Nil(t, err)
		assert.Equal(t, uint64(4<<18+i), now)
	}
	for i := 0; i < 5; i++ {
		now, err := hlcClock.GetTimestamp(context.TODO(), nil)
		assert.Nil(t, err)
		assert.Equal(t, uint64(5<<18+i), now)
	}
}
