package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	api "github.com/alphaleph/yojimbo/api/v1"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, l *Log){
		"append and read a record":    testAppendRead,
		"offset out of range error":   testOutOfRangeErr,
		"init with existing segments": testInitExisting,
		"reader":                      testReader,
		"truncate":                    testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "log-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			l, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, l)
		})
	}
}

func testAppendRead(t *testing.T, l *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	offset, err := l.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	read, err := l.Read(offset)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, l *Log) {
	read, err := l.Read(1)
	require.Nil(t, read)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, l *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := l.Append(append)
		require.NoError(t, err)
	}
	require.NoError(t, l.Close())

	offset, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)
	offset, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), offset)

	l2, err := NewLog(l.Dir, l.Config)
	require.NoError(t, err)

	offset, err = l2.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)
	offset, err = l2.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), offset)
}

func testReader(t *testing.T, l *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	offset, err := l.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)
	r := l.Reader()
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

func testTruncate(t *testing.T, l *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := l.Append(append)
		require.NoError(t, err)
	}
	err := l.Truncate(1)
	require.NoError(t, err)
	_, err = l.Read(0)
	require.NoError(t, err)
}
