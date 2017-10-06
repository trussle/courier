package fs

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/trussle/courier/pkg/fs/ioext"
	"github.com/trussle/courier/pkg/fs/lock"
	"github.com/trussle/courier/pkg/fs/mmap"
)

const mkdirAllMode = 0755

type localFilesystem struct {
	mmap bool
}

// NewLocalFilesystem yields a local disk filesystem.
func NewLocalFilesystem(mmap bool) Filesystem {
	return localFilesystem{mmap}
}

func (localFilesystem) Create(path string) (File, error) {
	f, err := os.Create(path)
	return localFile{
		File:   f,
		Reader: f,
		Closer: f,
	}, err
}

func (fs localFilesystem) Open(path string) (File, error) {
	f, err := os.Open(path)
	if err != nil {
		if err == os.ErrNotExist {
			return nil, errNotFound{err}
		}
		return nil, err
	}

	local := localFile{
		File:   f,
		Reader: f,
		Closer: f,
	}

	if fs.mmap {
		r, err := mmap.New(f)
		if err != nil {
			return nil, err
		}
		local.Reader = ioext.OffsetReader(r, 0)
		local.Closer = multiCloser{r, f}
	}

	return local, nil
}

func (localFilesystem) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (localFilesystem) Exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func (localFilesystem) Remove(path string) error {
	return os.Remove(path)
}

func (localFilesystem) MkdirAll(path string) error {
	return os.MkdirAll(path, mkdirAllMode)
}

func (localFilesystem) Chtimes(path string, atime, mtime time.Time) error {
	return os.Chtimes(path, atime, mtime)
}

func (localFilesystem) Walk(root string, walkFn filepath.WalkFunc) error {
	return filepath.Walk(root, walkFn)
}

func (localFilesystem) Lock(path string) (r Releaser, existed bool, err error) {
	r, existed, err = lock.New(path)
	r = deletingReleaser{path, r}
	return r, existed, err
}

type localFile struct {
	*os.File
	io.Reader
	io.Closer
}

func (f localFile) Read(p []byte) (int, error) {
	return f.Reader.Read(p)
}

func (f localFile) Close() error {
	return f.Closer.Close()
}

func (f localFile) Size() int64 {
	fi, err := f.File.Stat()
	if err != nil {
		panic(err)
	}
	return fi.Size()
}

type deletingReleaser struct {
	path string
	r    Releaser
}

func (dr deletingReleaser) Release() error {
	// Remove before Release should be safe, and prevents a race.
	if err := os.Remove(dr.path); err != nil {
		return err
	}
	return dr.r.Release()
}

// multiCloser closes all underlying io.Closers.
// If an error is encountered, closings continue.
type multiCloser []io.Closer

func (c multiCloser) Close() error {
	var errs []error
	for _, closer := range c {
		if closer == nil {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return multiCloseError(errs)
	}
	return nil
}

type multiCloseError []error

func (e multiCloseError) Error() string {
	a := make([]string, len(e))
	for i, err := range e {
		a[i] = err.Error()
	}
	return strings.Join(a, "; ")
}
