package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8 // number of bytes used to store a record's length
)

// abstraction to handle reading and writing data to and from disk
type store struct {
	File *os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fStat, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fStat.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Writes data to the store's buffer, returns the total number of bytes written to the buffer,
// where the record starts, and error.
//
// Note - writes to buf instead of to file to reduce total system calls (good for dealing with
// high volumes of small messages), but this means that data is not written to storage in this call
func (s *store) Append(data []byte) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos := s.size
	// Write size of data so that we know how far to read for this message.
	// This is written as the binary representation of the uint64 length, so it
	// will always be 64 bits in length.
	if err := binary.Write(s.buf, enc, uint64(len(data))); err != nil {
		return 0, 0, err
	}
	bytesWritten, err := s.buf.Write(data) // write the data itself
	if err != nil {
		return 0, 0, err
	}
	bytesWritten += lenWidth // bytes written + offset for storing record length
	s.size += uint64(bytesWritten)
	return uint64(bytesWritten), pos, nil

}

// Read a record at a given position. Returns a byte slice containing the record, and err
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth) // get size of our record
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// make byte slice of record size and start read after lenWidth offset
	recordSlice := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(recordSlice, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return recordSlice, nil
}

// Implements `ReadAt` on store with mutex and buffer flush. ReadAt reads len(b) bytes
// starting at the offset, and writes them to byte slice b. It returns the number of bytes
// read and error.
func (s *store) ReadAt(b []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(b, offset)
}

// Persist buffered data before closing file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
