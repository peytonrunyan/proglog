package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4 // record's offset, stored as int32
	posWidth uint64 = 8 // record's position, stored as uint64

	// The length of each record in our index. We can jump to a given record in the
	// index by going to byte entryWidth*offset, e.g. the fourth record is at entryWidth*4
	entryWidth = offWidth + posWidth
)

// Struct for our index. Holds a persistent index file and a memory mapped file
// Size is the size of the index file and tells us where our next entry should be appended
type index struct {
	file *os.File    // index file
	mmap gommap.MMap // memory mapped index file
	size uint64      // size of the index file - where our next entry should be appended
}

// Creates an index for the given file. The file's size is truncated to the
// max length specified in the config (truncate will grow it to that size if it is
// shorter than the specified length) and then the file is memory mapped before the
// index is returned.
//
// Details: We truncate the file to max size because we cannot change the size of a file
// that has been memory mapped
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}

	fStat, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fStat.Size())                             // where to resume
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)) // max size of file
	if err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map( // memory map the file
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED, // let forked processes access the mmap
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Closes the file and persists the data to storage. It will also resize the file
// from the max file size to the size of the written contents to ensure that reads and
// writes begin from the correct location.
func (idx *index) Close() error {
	// sync memory-mapped file to persisted file
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// ensure that everything is written to stable storage
	if err := idx.file.Sync(); err != nil {
		return err
	}
	// move from max size to size of written contents
	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return err
	}
	return idx.file.Close()
}

// Get the store position for an entry at a given offset in our index. Use -1 to get the last
// entry. Returns the offset that was used, the entry's position in the store, and err.
func (idx *index) Read(offsetGiven int64) (offsetUsed uint32, storePosition uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}
	if offsetGiven == -1 { // position of the last entry
		offsetUsed = (uint32(idx.size) / uint32(entryWidth)) - 1
	} else {
		offsetUsed = uint32(offsetGiven) // 0 indexed, so offset*entryWidth = start of entry
	}
	// make sure we've actually got 12 bytes to read
	if (uint64(offsetUsed)*entryWidth)+entryWidth > idx.size {
		return 0, 0, io.EOF
	}
	entryStart := uint64(offsetUsed) * entryWidth
	// set offset to actual listed offset at entry location
	offsetUsed = enc.Uint32(idx.mmap[entryStart:offWidth])
	// get last 8 bits of the entry
	storePosition = enc.Uint64(idx.mmap[entryStart+offWidth : entryStart+entryWidth])
	return offsetUsed, storePosition, nil
}

// Appends an entry with the provided offset and store location to the mmap index. Returns err.
func (idx *index) Write(offset uint32, storePosition uint64) error {
	if uint64(len(idx.mmap)) < (uint64(idx.size) + entryWidth) { // check for room
		return io.EOF
	}
	enc.PutUint32(idx.mmap[idx.size:idx.size+offWidth], offset)
	enc.PutUint64(idx.mmap[idx.size+offWidth:idx.size+entryWidth], storePosition)
	idx.size += uint64(entryWidth)
	return nil
}

// Return the filename of our index's persistent file.
func (idx *index) Name() string {
	return idx.file.Name()
}
