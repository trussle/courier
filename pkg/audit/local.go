package audit

import (
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/trussle/fsys"
)

//Extension describe differing types of persisted queued types
type Extension string

const (

	// Flushed states which items have been flushed
	Flushed Extension = ".flushed"
)

// Ext returns the extension of the constant extension
func (e Extension) Ext() string {
	return string(e)
}

const (
	lockFile = "LOCK"
)

// LocalConfig creates a configuration to create a LocalLog.
type LocalConfig struct {
	RootPath string
	Fsys     fsys.Filesystem
}

// Log represents a series of active records
type localLog struct {
	root   string
	fsys   fsys.Filesystem
	logger log.Logger
}

// NewLocalLog creates a new Log with a size and age to know when a
// Log is at a certain capacity
func newLocalLog(config *LocalConfig, logger log.Logger) (Log, error) {
	var (
		fsys = config.Fsys
		root = config.RootPath
	)
	if err := fsys.MkdirAll(root); err != nil {
		return nil, errors.Wrapf(err, "creating path %s", root)
	}

	lock := filepath.Join(root, lockFile)
	r, _, err := fsys.Lock(lock)
	if err != nil {
		return nil, errors.Wrapf(err, "locking %s", lock)
	}
	defer r.Release()
}
