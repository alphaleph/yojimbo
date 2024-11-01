package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/alphaleph/yojimbo/api/v1"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	expected := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entryWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		offset, err := s.Append(expected)
		require.NoError(t, err)
		require.Equal(t, 16+i, offset)

		res, err := s.Read(offset)
		require.NoError(t, err)
		require.Equal(t, expected.Value, res.Value)
	}

	_, err = s.Append(expected)
	require.Equal(t, io.EOF, err)
	require.True(t, s.IsMaxed())

	// Check loading segment state from persisted index and log files
	c.Segment.MaxStoreBytes = uint64(len(expected.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())

}
