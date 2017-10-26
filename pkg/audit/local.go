package audit

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
	"github.com/trussle/fsys"
)

//Extension describe differing types of persisted queued types
type Extension string

const (

	// Active states which items are currently actively being worked on
	Active Extension = ".active"

	// Flushed states which items have been flushed
	Flushed Extension = ".flushed"

	// Failed status which items are failed
	Failed Extension = ".failed"
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

	return &localLog{
		root:   root,
		fsys:   fsys,
		logger: logger,
	}, nil
}

func (r *localLog) Append(txn models.Transaction) error {
	lock := filepath.Join(r.root, lockFile)
	releaser, _, err := r.fsys.Lock(lock)
	if err != nil {
		return errors.Wrapf(err, "locking %s", lock)
	}
	defer releaser.Release()

	var (
		timestamp = time.Now().Format(time.RFC3339Nano)
		fileName  = base64.RawURLEncoding.EncodeToString([]byte(timestamp))
		filePath  = filepath.Join(r.root, fileName)
	)

	file, err := generateFile(r.fsys, filePath, Active)
	if err != nil {
		return err
	}

	if err := txn.Walk(func(id uuid.UUID, record models.Record) error {
		_, e := file.Write(row(id, record))
		return e
	}); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	// Flush it
	var (
		oldname = file.Name()
		newname = modifyExtension(oldname, Flushed.Ext())
	)
	return r.fsys.Rename(oldname, newname)
}

func generateFile(fsys fsys.Filesystem, root string, ext Extension) (fsys.File, error) {
	filename := fmt.Sprintf("%s%s", root, ext.Ext())
	return fsys.Create(filename)
}

// Recover any active segments and make them failed segments.
func recoverSegments(filesys fsys.Filesystem, root string) error {
	var toRename []string
	filesys.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		switch filepath.Ext(path) {
		case Active.Ext():
			toRename = append(toRename, path)
		}
		return nil
	})

	for _, path := range toRename {
		var (
			oldname = path
			newname = modifyExtension(oldname, Failed.Ext())
		)
		if err := filesys.Rename(oldname, newname); err != nil {
			return err
		}
	}
	return nil
}

func modifyExtension(filename, newExt string) string {
	return filename[:len(filename)-len(filepath.Ext(filename))] + newExt
}


// LocalConfigOption defines a option for generating a LocalConfig
type LocalConfigOption func(*LocalConfig) error

// BuildLocalConfig ingests configuration options to then yield a
// LocalConfig, and return an error if it fails during configuring.
func BuildLocalConfig(opts ...LocalConfigOption) (*LocalConfig, error) {
	var config LocalConfig
	for _, opt := range opts {
		err := opt(&config)
		if err != nil {
			return nil, err
		}
	}
	return &config, nil
}

// WithRootPath adds an rootPath option to the configuration
func WithRootPath(rootPath string) LocalConfigOption {
	return func(config *LocalConfig) error {
		config.RootPath = rootPath
		return nil
	}
}

// WithFsys adds an fsys option to the configuration
func WithFsys(fsys fsys.Filesystem) LocalConfigOption {
	return func(config *LocalConfig) error {
		config.Fsys = fsys
		return nil
	}
}
