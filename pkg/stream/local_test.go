package stream

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"testing/quick"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/trussle/courier/pkg/fs"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/queue/mocks"
	"github.com/trussle/courier/pkg/uuid"
)

func TestLocalStream(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Run("create returns nil", func(t *testing.T) {
		fsys := fs.NewVirtualFilesystem()
		_, err := newLocalStream(fsys, "/root", 1, time.Second)

		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("new stream returns correct size", func(t *testing.T) {
		fsys := fs.NewVirtualFilesystem()
		stream, err := newLocalStream(fsys, "/root", 1, time.Second)

		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		if expected, actual := 0, stream.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("append sets the correct size", func(t *testing.T) {
		fn := func(record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			fsys := fs.NewVirtualFilesystem()
			stream, err := newLocalStream(fsys, "/root", 1, time.Second)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(record.ID)
			segment.EXPECT().Walk(Walk(record)).Return(nil)

			err = stream.Append(segment)
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if expected, actual := 1, stream.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			var called bool
			fsys.Walk("/root", func(path string, info os.FileInfo, err error) error {
				called = true

				fileName := fmt.Sprintf("/root/%s.active", record.ID.String())
				if expected, actual := fileName, path; expected != actual {
					t.Errorf("expected: %s, actual: %s", expected, actual)
				}
				return nil
			})

			return called
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("append then resets to the correct size", func(t *testing.T) {
		fn := func(record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			fsys := fs.NewVirtualFilesystem()
			stream, err := newLocalStream(fsys, "/root", 1, time.Second)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(record.ID)
			segment.EXPECT().Walk(Walk(record)).Return(nil)

			err = stream.Append(segment)
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if expected, actual := 1, stream.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			err = stream.Reset()

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if expected, actual := 0, stream.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("append capacity", func(t *testing.T) {
		fn := func(record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			fsys := fs.NewVirtualFilesystem()
			stream, err := newLocalStream(fsys, "/root", 1, time.Second)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(record.ID)
			segment.EXPECT().Walk(Walk(record)).Return(nil)

			err = stream.Append(segment)
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if expected, actual := true, stream.Capacity(); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk on empty stream yields no call", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fsys := fs.NewVirtualFilesystem()
		stream, err := newLocalStream(fsys, "/root", 1, time.Second)

		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		err = stream.Walk(func(queue.Segment) error {
			t.Fatal(errors.New("failed if called"))
			return nil
		})
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("walk on stream yields a segment", func(t *testing.T) {
		fn := func(record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(uuid.MustNew(rnd))
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(2)

			fsys := fs.NewVirtualFilesystem()
			stream, err := newLocalStream(fsys, "/root", 1, time.Second)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if err = stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			var called bool
			err = stream.Walk(func(s queue.Segment) error {
				return s.Walk(func(r queue.Record) error {
					called = true
					if expected, actual := record, r; !expected.Equals(actual) {
						t.Errorf("expected: %v, actual: %v", expected, actual)
					}
					return nil
				})
			})
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 1, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := true, called; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("commit", func(t *testing.T) {
		fn := func(record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			id := uuid.MustNew(rnd)
			ids := []uuid.UUID{
				record.ID,
			}

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(id).Times(3)
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(2)
			segment.EXPECT().Commit(CompareUUIDs(ids)).Return(queue.Result{}, nil)

			fsys := fs.NewVirtualFilesystem()
			stream, err := newLocalStream(fsys, "/root", 1, time.Second)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if err = stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			input := NewTransaction()
			input.Set(id, ids)

			err = stream.Commit(input)
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 0, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			var called bool
			fsys.Walk("/root", func(path string, info os.FileInfo, err error) error {
				called = true

				fileName := fmt.Sprintf("/root/%s.flushed", id.String())
				if expected, actual := fileName, path; expected != actual {
					t.Errorf("expected: %s, actual: %s", expected, actual)
				}
				return nil
			})

			return called
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("commit with wildcard", func(t *testing.T) {
		fn := func(record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			id := uuid.MustNew(rnd)
			ids := []uuid.UUID{
				record.ID,
			}

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(id).Times(2)
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(3)
			segment.EXPECT().Commit(CompareUUIDs(ids)).Return(queue.Result{}, nil)

			fsys := fs.NewVirtualFilesystem()
			stream, err := newLocalStream(fsys, "/root", 1, time.Second)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if err = stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			err = stream.Commit(All())
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 0, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			var called bool
			fsys.Walk("/root", func(path string, info os.FileInfo, err error) error {
				called = true

				fileName := fmt.Sprintf("/root/%s.flushed", id.String())
				if expected, actual := fileName, path; expected != actual {
					t.Errorf("expected: %s, actual: %s", expected, actual)
				}
				return nil
			})

			return called
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("failed", func(t *testing.T) {
		fn := func(record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			id := uuid.MustNew(rnd)
			ids := []uuid.UUID{
				record.ID,
			}

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(id).Times(3)
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(2)
			segment.EXPECT().Failed(CompareUUIDs(ids)).Return(queue.Result{}, nil)

			fsys := fs.NewVirtualFilesystem()
			stream, err := newLocalStream(fsys, "/root", 1, time.Second)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if err = stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			input := NewTransaction()
			input.Set(id, ids)

			err = stream.Failed(input)
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 0, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("failed with wildcard", func(t *testing.T) {
		fn := func(record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			id := uuid.MustNew(rnd)
			ids := []uuid.UUID{
				record.ID,
			}

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(id).Times(2)
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(3)
			segment.EXPECT().Failed(CompareUUIDs(ids)).Return(queue.Result{}, nil)

			fsys := fs.NewVirtualFilesystem()
			stream, err := newLocalStream(fsys, "/root", 1, time.Second)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if err = stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			err = stream.Failed(All())
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 0, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestExtension(t *testing.T) {
	t.Parallel()

	t.Run("active", func(t *testing.T) {
		if expected, actual := ".active", Active.Ext(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})

	t.Run("flushed", func(t *testing.T) {
		if expected, actual := ".flushed", Flushed.Ext(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})

	t.Run("failed", func(t *testing.T) {
		if expected, actual := ".failed", Failed.Ext(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})
}

func TestContains(t *testing.T) {
	t.Parallel()

	t.Run("contains", func(t *testing.T) {
		fn := func(ids []uuid.UUID) bool {
			if len(ids) == 0 {
				return true
			}
			return contains(ids, ids[rand.Intn(len(ids))])
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("contains nothing", func(t *testing.T) {
		fn := func(ids []uuid.UUID) bool {
			if len(ids) == 0 {
				return true
			}
			return !contains(ids, uuid.Empty)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestModifyExtension(t *testing.T) {
	t.Parallel()

	t.Run("modify extension", func(t *testing.T) {
		res := modifyExtension("filename.a", ".b")
		if expected, actual := "filename.b", res; expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})

	t.Run("modify extension with folder", func(t *testing.T) {
		res := modifyExtension("folder/filename.a", ".b")
		if expected, actual := "folder/filename.b", res; expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})
}

func TestGenerateFile(t *testing.T) {
	fsys := fs.NewVirtualFilesystem()

	if _, err := generateFile(fsys, "/root", Active); err != nil {
		t.Error(err)
	}

	var called bool
	fsys.Walk("/root", func(path string, info os.FileInfo, err error) error {
		called = true

		if expected, actual := ".active", filepath.Ext(path); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
		return nil
	})

	if expected, actual := true, called; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestRecoverSegments(t *testing.T) {
	fsys := fs.NewVirtualFilesystem()
	fsys.Create("/root/filename.active")

	if err := recoverSegments(fsys, "/root"); err != nil {
		t.Error(err)
	}

	var called bool
	fsys.Walk("/root", func(path string, info os.FileInfo, err error) error {
		called = true

		if expected, actual := "/root/filename.failed", path; expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
		return nil
	})

	if expected, actual := true, called; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}
