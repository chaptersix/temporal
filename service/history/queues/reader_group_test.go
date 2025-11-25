package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
)

type (
	testReader struct {
		status int32
	}
)

func TestReaderGroupStartStop(t *testing.T) {
	t.Parallel()

	readerGroup := NewReaderGroup(
		func(_ int64, _ []Slice) Reader {
			return newTestReader()
		},
	)

	readerID := DefaultReaderId
	r := readerGroup.NewReader(readerID)
	require.Equal(t, common.DaemonStatusInitialized, r.(*testReader).status)

	readerGroup.Start()
	require.Equal(t, common.DaemonStatusStarted, r.(*testReader).status)

	readerID = DefaultReaderId + 1
	r = readerGroup.NewReader(readerID)
	require.Equal(t, common.DaemonStatusStarted, r.(*testReader).status)

	var readers []*testReader
	for _, reader := range readerGroup.Readers() {
		readers = append(readers, reader.(*testReader))
	}
	readerGroup.Stop()
	require.Len(t, readers, 2)
	for _, r := range readers {
		require.Equal(t, common.DaemonStatusStopped, r.status)
	}

	readerID = DefaultReaderId + 2
	r = readerGroup.NewReader(readerID)
	require.Equal(t, common.DaemonStatusInitialized, r.(*testReader).status)
}

func TestReaderGroupAddGetReader(t *testing.T) {
	t.Parallel()

	readerGroup := NewReaderGroup(
		func(_ int64, _ []Slice) Reader {
			return newTestReader()
		},
	)

	require.Empty(t, readerGroup.Readers())

	r, ok := readerGroup.ReaderByID(DefaultReaderId)
	require.False(t, ok)
	require.Nil(t, r)

	for i := int64(0); i < 3; i++ {
		r := readerGroup.NewReader(i)

		readers := readerGroup.Readers()
		require.Len(t, readers, int(i)+1)
		require.Equal(t, r, readers[i])

		retrievedReader, ok := readerGroup.ReaderByID(i)
		require.True(t, ok)
		require.Equal(t, r, retrievedReader)
	}

	require.Panics(t, func() {
		readerGroup.NewReader(DefaultReaderId)
	})
}

func TestReaderGroupRemoveReader(t *testing.T) {
	t.Parallel()

	readerGroup := NewReaderGroup(
		func(_ int64, _ []Slice) Reader {
			return newTestReader()
		},
	)

	readerGroup.Start()
	defer readerGroup.Stop()

	readerID := DefaultReaderId

	r := readerGroup.NewReader(readerID)
	readerGroup.RemoveReader(readerID)

	require.Equal(t, common.DaemonStatusStopped, r.(*testReader).status)
	require.Len(t, readerGroup.Readers(), 0)
}

func TestReaderGroupForEach(t *testing.T) {
	t.Parallel()

	readerGroup := NewReaderGroup(
		func(_ int64, _ []Slice) Reader {
			return newTestReader()
		},
	)

	readerIDs := []int64{1, 2, 3}
	for _, readerID := range readerIDs {
		_ = readerGroup.NewReader(readerID)
	}

	forEachResult := make(map[int64]Reader)
	readerGroup.ForEach(func(i int64, r Reader) {
		forEachResult[i] = r
	})

	require.Equal(t, readerGroup.Readers(), forEachResult)
}

func newTestReader() Reader {
	return &testReader{
		status: common.DaemonStatusInitialized,
	}
}

func (r *testReader) Start()                       { r.status = common.DaemonStatusStarted }
func (r *testReader) Stop()                        { r.status = common.DaemonStatusStopped }
func (r *testReader) Scopes() []Scope              { panic("not implemented") }
func (r *testReader) WalkSlices(SliceIterator)     { panic("not implemented") }
func (r *testReader) SplitSlices(SliceSplitter)    { panic("not implemented") }
func (r *testReader) MergeSlices(...Slice)         { panic("not implemented") }
func (r *testReader) AppendSlices(...Slice)        { panic("not implemented") }
func (r *testReader) ClearSlices(SlicePredicate)   { panic("not implemented") }
func (r *testReader) CompactSlices(SlicePredicate) { panic("not implemented") }
func (r *testReader) ShrinkSlices() int            { panic("not implemented") }
func (r *testReader) Notify()                      { panic("not implemented") }
func (r *testReader) Pause(time.Duration)          { panic("not implemented") }
