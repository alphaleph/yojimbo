package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Offset uint32
		Pos    uint64
	}{
		{Offset: 0, Pos: 0},
		{Offset: 1, Pos: 10},
	}
	for _, expected := range entries {
		err = idx.Write(expected.Offset, expected.Pos)
		require.NoError(t, err)
		_, pos, err := idx.Read(int64(expected.Offset))
		require.NoError(t, err)
		require.Equal(t, expected.Pos, pos)
	}

	// Index and Scanner should error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// Index should init from existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	offset, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), offset)
	require.Equal(t, entries[1].Pos, pos)
}
