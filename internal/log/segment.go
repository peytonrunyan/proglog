// Package for segments, which coordinates operations between the index and the store
package log

import (
	"fmt"
	"os"
	"path"

	"github.com/golang/protobuf/proto"
	api "github.com/peytonrunyan/proglog/api/v1"
)

type segment struct {
	store      *store // pointer to this segment's store
	index      *index // pointer to this segement's index
	baseOffset uint64 // where our data starts in our INDEX FILE
	nextOffset uint64 // where to append the next entry in our INDEX FILE
	config     Config // info about max store and index file size
}

// Called when a new segment needs to be added (e.g. when the current segment reaches its max size).
// This will create a new index file and a new store file in addition to returning the segment.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	// Create new store file, labeled with baseOffset
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	// Create new index file, labeled with baseoffset
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	// Tries to read last element of the index file. If the index is new, there won't be
	// anything to read and this will return an error, so the nextOffset (next location
	// to write) should be the base offset. Otherwise the next position should be the next
	// byte after the end of last item.
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = s.baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Writes record to segment and returns the offset of the appended record.
// This writes to the store's buffer and updates the index file with the offset
// and position of the record.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	recordOffset := s.nextOffset
	record.Offset = recordOffset
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	// append record to store
	_, recordStart, err := s.store.Append(p)
	// update index to reflect newly appended record
	if err = s.index.Write(
		// index offset relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		recordStart,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return recordOffset, nil
}

// Reads entry at a given offset by converting the offset to an index offset,
// and then reading from the location in the store file indicated by the index.
func (s *segment) Read(offset uint64) (*api.Record, error) {
	_, storePosition, err := s.index.Read(int64(offset - s.baseOffset))
	if err != nil {
		return nil, err
	}
	entry, err := s.store.Read(storePosition)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(entry, record)
	return record, err
}

// Check if we have exceeded limits for either our index or store. Returns bool.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Close the segment and delete its associated index and store files. Returns err.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close the index and the store associated with the segment
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// Returns the multiple of desiredMultiple nearest to the value of numToCheck.
// TODO Used to determine index position I think (because of how negatives behave).
// Examples:
//	- numToCheck = 10, desiredMultiple = 2, result = 10
// 	- numToCheck = 6, desiredMultiple = 4, results = 4
//	- numToCheck = -4, desiredMultiple = 2, result = -10
func (s *segment) nearestMultiple(numToCheck, desiredMultiple uint64) uint64 {
	if numToCheck < 0 {
		return ((numToCheck - desiredMultiple + 1) / desiredMultiple) * desiredMultiple
	}
	return (numToCheck / desiredMultiple) * desiredMultiple
}
