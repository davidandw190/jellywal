package jellywal

import "os"

const (
	DefaultSegmentSize = 20 * 1024 * 1024 // 20 MB
	DefaultDirPerms    = 0750
	DefaultFilePerms   = 0640
)

// Options for configuring the log
type Config struct {
	Sync        bool        // Enable fsync after writes for more durability
	SegmentSize int         // Size of each log segment. Default is 20 MB.
	DirPerms    os.FileMode // Directory permissions.
	FilePerms   os.FileMode // Log file permissions.
}

// DefaultOptions for the log
var Defaultconfig = &Config{
	Sync:        true, // Fsync after every write
	SegmentSize: DefaultSegmentSize,
	DirPerms:    DefaultDirPerms,
	FilePerms:   DefaultFilePerms,
}
