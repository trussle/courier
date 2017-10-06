package fs

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestLocalFilesystem(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fsys := NewLocalFilesystem(false)
		testFilesystemCreate(fsys, dir, t)
	})

	t.Run("open", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fsys := NewLocalFilesystem(false)
		testFilesystemOpen(fsys, dir, t)
	})

	t.Run("rename", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fsys := NewLocalFilesystem(false)
		testFilesystemRename(fsys, dir, t)
	})

	t.Run("exists", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fsys := NewLocalFilesystem(false)
		testFilesystemExists(fsys, dir, t)
	})

	t.Run("remove", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fsys := NewLocalFilesystem(false)
		testFilesystemRemove(fsys, dir, t)
	})

	t.Run("walk", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fsys := NewLocalFilesystem(false)
		testFilesystemWalk(fsys, dir, t)
	})
}

func TestLocalFile(t *testing.T) {
	t.Parallel()

	t.Run("name", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fsys := NewLocalFilesystem(false)
		testFileName(fsys, dir, t)
	})

	t.Run("size", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fsys := NewLocalFilesystem(false)
		testFileSize(fsys, dir, t)
	})

	t.Run("read and write", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fsys := NewLocalFilesystem(false)
		testFileReadWrite(fsys, dir, t)
	})
}
