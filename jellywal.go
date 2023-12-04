package jellywal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	DefaultSegmentSize = 20 * 1024 * 1024 // 20 MB
	DefaultDirPerms    = 0750
	DefaultFilePerms   = 0640
)

// Config for configuring the log
type Config struct {
	Sync        bool        // Enable fsync after writes for more durability
	SegmentSize int         // Size of each log segment. Default is 20 MB.
	DirPerms    os.FileMode // Directory permissions.
	FilePerms   os.FileMode // Log file permissions.
}

// DefaultConfig for the log
var DefaultConfig = &Config{
	Sync:        true, // Fsync after every write
	SegmentSize: DefaultSegmentSize,
	DirPerms:    DefaultDirPerms,
	FilePerms:   DefaultFilePerms,
}

// Log represents a write-ahead log, also known as an append only log
type Log struct {
	mu       sync.RWMutex
	path     string     // Absolute path to log directory
	segments []*segment // All known log segments
	sfile    *os.File   // Tail segment file handle
	wbatch   Batch      // Reusable write batch

	config  Config
	closed  bool
	corrupt bool
}

// Segment represents a single segment file.
type segment struct {
	path  string    // Path of the segment file
	index uint64    // First index of the segment
	cbuf  []byte    // Cached entries buffer
	cpos  []bytepos // Cached entries positions in the buffer
}

// bpos represents byte positions in a buffer
type bytepos struct {
	start int // Byte position
	end   int // One byte past pos
}

type Batch struct {
	entries []batchEntry
	datas   []byte
}

type batchEntry struct {
	size int
}

func (c *Config) Validate() {
	if c.SegmentSize <= 0 {
		c.SegmentSize = DefaultSegmentSize
	}

	if c.DirPerms == 0 {
		c.DirPerms = DefaultDirPerms
	}

	if c.FilePerms == 0 {
		c.FilePerms = DefaultFilePerms
	}
}

// loadSegments loads existing log segments from the log directory.
func (l *Log) loadSegments() error {
	files, err := os.ReadDir(l.path)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	for _, file := range files {
		name := file.Name()

		if file.IsDir() || len(name) < 20 {
			continue
		}

		index, err := strconv.ParseUint(name[:20], 10, 64)
		if err != nil || index == 0 {
			continue
		}

		if len(name) == 20 {
			segment := &segment{
				index: index,
				path:  filepath.Join(l.path, name),
			}
			l.segments = append(l.segments, segment)
		}
	}

	if len(l.segments) == 0 {
		// Create a new log in this case
		if err := l.createInitialSegment(); err != nil {
			return fmt.Errorf("failed to create initial log segment: %w", err)
		}
	} else {
		// Open the last segment for appending
		lastSegment := l.segments[len(l.segments)-1]
		if err := l.openLastSegment(lastSegment); err != nil {
			return fmt.Errorf("failed to open last log segment: %w", err)
		}
	}

	return nil
}

func (l *Log) createInitialSegment() error {
	initialSegment := &segment{
		index: 1,
		path:  filepath.Join(l.path, segmentName(1)),
	}

	l.segments = append(l.segments, initialSegment)

	file, err := os.OpenFile(initialSegment.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.config.FilePerms)
	if err != nil {
		return fmt.Errorf("failed to create initial log segment file: %w", err)
	}

	l.sfile = file

	return nil
}

// openLastSegment opens the last log segment for appending.
func (l *Log) openLastSegment(lastSegment *segment) error {
	file, err := os.OpenFile(lastSegment.path, os.O_WRONLY, l.config.FilePerms)
	if err != nil {
		return fmt.Errorf("failed to open last log segment file: %w", err)
	}

	l.sfile = file

	if _, err := l.sfile.Seek(0, 2); err != nil {
		return fmt.Errorf("failed to seek in last log segment file: %w", err)
	}

	// Load the last segment entries
	if err := l.loadSegmentEntries(lastSegment); err != nil {
		return fmt.Errorf("failed to load last log segment entries: %w", err)
	}

	return nil
}

func segmentName(index uint64) string {
	return fmt.Sprintf("%020d", index)
}

// loadSegmentEntries reads entries from the specified log segment file and populates the segment.
func (l *Log) loadSegmentEntries(segment *segment) error {
	data, err := os.ReadFile(segment.path)
	if err != nil {
		return fmt.Errorf("failed to read log segment file: %w", err)
	}

	entryBuffer := data
	var entryPositions []bytepos
	var currentPosition int

	for len(data) > 0 {
		var bytesRead int
		bytesRead, err = l.loadNextBinaryEntry(data)
		if err != nil {
			return fmt.Errorf("failed to load binary entry from log segment: %w", err)
		}

		data = data[bytesRead:]
		entryPositions = append(entryPositions, bytepos{currentPosition, currentPosition + bytesRead})
		currentPosition += bytesRead
	}

	segment.cbuf = entryBuffer
	segment.cpos = entryPositions
	return nil
}

// loadNextBinaryEntry reads the size of the next binary entry and returns the number of bytes read.
func (l *Log) loadNextBinaryEntry(data []byte) (int, error) {
	// data_size + data
	size, bytesRead := binary.Uvarint(data)
	if bytesRead <= 0 {
		return 0, ErrCorrupt
	}
	if uint64(len(data)-bytesRead) < size {
		return 0, ErrCorrupt
	}
	return bytesRead + int(size), nil
}
