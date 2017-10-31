package audit

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/uuid"
	"github.com/trussle/fsys"
)

func TestLocal(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Run("append", func(t *testing.T) {
		virtual := fsys.NewVirtualFilesystem()
		config, err := BuildLocalConfig(
			WithRootPath(""),
			WithFsys(virtual),
		)
		if err != nil {
			t.Fatal(err)
		}

		localLog, err := newLocalLog(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		id, err := uuid.New(rnd)
		if err != nil {
			t.Fatal(err)
		}

		record, err := queue.GenerateQueueRecord(rnd)
		if err != nil {
			t.Fatal(err)
		}

		txn := queue.NewTransaction()
		txn.Push(id, record)

		if err := localLog.Append(txn); err != nil {
			t.Fatal(err)
		}

		if err := virtual.Walk("", func(path string, info os.FileInfo, err error) error {
			file, err := virtual.Open(path)
			if err != nil {
				return err
			}

			bytes, err := ioutil.ReadAll(file)
			if err != nil {
				return err
			}

			if expected, actual := record.RecordID(), strings.Split(string(bytes), " ")[0]; expected != actual {
				t.Errorf("expected: %s, actual: %s", expected, actual)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBuildLocalConfig(t *testing.T) {
	t.Parallel()

	t.Run("build", func(t *testing.T) {
		fn := func(path string) bool {
			config, err := BuildLocalConfig(
				WithRootPath(path),
				WithFsys(fsys.NewNopFilesystem()),
			)
			if err != nil {
				t.Fatal(err)
			}
			return config.RootPath == path
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid build", func(t *testing.T) {
		_, err := BuildLocalConfig(
			func(config *LocalConfig) error {
				return errors.Errorf("bad")
			},
		)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
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
	fsys := fsys.NewVirtualFilesystem()

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
	fsys := fsys.NewVirtualFilesystem()
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
