package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

// Since Append returns num bytes written and the previous starting position, we expect that
// after each write of the same string, the previous position + the number of bytes written
// will equal the size of item written*<the num times written>. This will be 8 bytes larger
// than the length of written bytes because the start of each write is the length of the item
// as a uint64
func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

// This uses the same logic as testAppend. We are writing a fixed-length string multiple times,
// so we expect to be able to find it consistently.
func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth) // reading the length of the record, not the record itself
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n) // bytes read should be equal to the length of the record

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off+lenWidth)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)

	}
}

func testClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	// make sure that write buffer was flushed
	require.Equal(t, afterSize, (beforeSize + int64(width)))
}

// helper function to deal with opening files
func openFile(fname string) (file *os.File, size int64, err error) {
	file, err = os.OpenFile(
		fname,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fStat, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	return file, fStat.Size(), nil
}
