package stream

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/queue"
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

// Stream represents a series of active records
type localStream struct {
	randSource  *rand.Rand
	root        string
	fsys        fsys.Filesystem
	active      []queue.Segment
	activeSince time.Time
	targetSize  int
	targetAge   time.Duration
}

// NewLocalStream creates a new Stream with a size and age to know when a
// Stream is at a certain capacity
func newLocalStream(fsys fsys.Filesystem, root string, size int, age time.Duration) (*localStream, error) {
	if err := fsys.MkdirAll(root); err != nil {
		return nil, errors.Wrapf(err, "creating path %s", root)
	}

	lock := filepath.Join(root, lockFile)
	r, _, err := fsys.Lock(lock)
	if err != nil {
		return nil, errors.Wrapf(err, "locking %s", lock)
	}
	defer r.Release()

	if err := recoverSegments(fsys, root); err != nil {
		return nil, errors.Wrap(err, "during recovery")
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	return &localStream{
		randSource:  rnd,
		root:        root,
		fsys:        fsys,
		active:      make([]queue.Segment, 0),
		activeSince: time.Time{},
		targetSize:  size,
		targetAge:   age,
	}, nil
}

// Len returns the number of available active records with in the Stream
func (s *localStream) Len() int {
	return len(s.active)
}

// Reset empties the localStream and puts it to a valid known state
func (s *localStream) Reset() error {
	s.active = s.active[:0]
	s.activeSince = time.Time{}

	return nil
}

// Capacity defines if the localStream is at a capacity. This is defined as if the
// localStream is over the target or age.
func (s *localStream) Capacity() bool {
	return s.Len() >= s.targetSize ||
		!s.activeSince.IsZero() && time.Since(s.activeSince) >= s.targetAge
}

// Append adds a segment with records to the localStream
func (s *localStream) Append(segment queue.Segment) error {
	lock := filepath.Join(s.root, lockFile)
	r, _, err := s.fsys.Lock(lock)
	if err != nil {
		return errors.Wrapf(err, "locking %s", lock)
	}
	defer r.Release()

	fileName := filepath.Join(s.root, segment.ID().String())
	file, err := generateFile(s.fsys, fileName, Active)
	if err != nil {
		return err
	}

	err = segment.Walk(func(rec queue.Record) error {
		_, e := file.Write(rec.Body)
		return e
	})

	s.active = append(s.active, segment)

	return err
}

// Walk allows the walking over each record sequentially
// If the localStreamger contains items that can no longer be walked over
func (s *localStream) Walk(fn func(queue.Segment) error) (err error) {
	for _, v := range s.active {
		if err = fn(v); err != nil {
			return
		}
	}
	return
}

// Commit commits all read segments that have been worked on via Walk, so that
// we can delete messages from the queue
func (s *localStream) Commit(input *Transaction) error {
	return s.resetVia(input, Flushed)
}

// Failed fails all segments that have been worked on via Walk. To make sure
// that we no longer work on those messages
func (s *localStream) Failed(input *Transaction) error {
	return s.resetVia(input, Failed)
}

func (s *localStream) resetVia(input *Transaction, reason Extension) error {
	lock := filepath.Join(s.root, lockFile)
	r, _, err := s.fsys.Lock(lock)
	if err != nil {
		return errors.Wrapf(err, "locking %s", lock)
	}
	defer r.Release()

	var segments []queue.Segment
	err = s.Walk(func(segment queue.Segment) error {
		var ids []uuid.UUID
		if input.All() {
			if err := segment.Walk(func(record queue.Record) error {
				ids = append(ids, record.ID)
				return nil
			}); err != nil {
				return nil
			}
		} else {
			var ok bool
			if ids, ok = input.Get(segment.ID()); !ok {
				segments = append(segments, segment)
				return nil
			}
		}

		// Select records that we actual need
		var records []queue.Record
		if err := segment.Walk(func(record queue.Record) error {
			if contains(ids, record.ID) {
				records = append(records, record)
			}
			return nil
		}); err != nil {
			return err
		}

		if len(records) == 0 {
			return nil
		}

		fileName := filepath.Join(s.root, segment.ID().String())

		switch reason {
		case Failed:
			if _, err := segment.Failed(ids); err != nil {
				return err
			}
		case Flushed:
			if _, err := segment.Commit(ids); err != nil {
				return err
			}

			file, err := generateFile(s.fsys, fileName, reason)
			if err != nil {
				return err
			}

			defer file.Close()

			for _, v := range records {
				if _, err := file.Write(append(v.Body, '\n')); err != nil {
					return err
				}
			}

			if err := file.Sync(); err != nil {
				return err
			}
		}

		return s.fsys.Remove(fmt.Sprintf("%s%s", fileName, Active.Ext()))
	})

	s.active = segments
	s.activeSince = time.Time{}

	return nil
}

func contains(ids []uuid.UUID, id uuid.UUID) bool {
	for _, v := range ids {
		if v.Equals(id) {
			return true
		}
	}
	return false
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
