package jellywal

import (
	"os"
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
