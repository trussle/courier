package fs

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestVirtualFilesystem(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fsys := NewVirtualFilesystem()
		testFilesystemCreate(fsys, dir, t)
	})

	t.Run("open", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fsys := NewVirtualFilesystem()
		testFilesystemOpen(fsys, dir, t)
	})

	t.Run("open with failure", func(t *testing.T) {
		var (
			dir          = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fsys         = NewVirtualFilesystem()
			path         = filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
			tmpfile, err = fsys.Create(path)
		)
		if err != nil {
			t.Error(err)
		}

		if f, ok := fsys.(*virtualFilesystem); ok {
			delete(f.files, path)
		}

		content := make([]byte, rand.Intn(1000)+100)
		if _, err = rand.Read(content); err != nil {
			t.Fatal(err)
		}
		if _, err = tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}

		defer fsys.Remove(tmpfile.Name())
		if _, err = tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err = tmpfile.Close(); err != nil {
			t.Fatal(err)
		}

		_, err = fsys.Open(path)
		if expected, actual := true, ErrNotFound(err); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("rename", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fsys := NewVirtualFilesystem()
		testFilesystemRename(fsys, dir, t)
	})

	t.Run("rename with failure", func(t *testing.T) {
		var (
			dir          = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fsys         = NewVirtualFilesystem()
			oldPath      = filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
			newPath      = filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
			tmpfile, err = fsys.Create(oldPath)
		)
		if err != nil {
			t.Error(err)
		}

		if f, ok := fsys.(*virtualFilesystem); ok {
			delete(f.files, oldPath)
		}

		content := make([]byte, rand.Intn(1000)+100)
		if _, err = rand.Read(content); err != nil {
			t.Fatal(err)
		}
		if _, err = tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}

		defer fsys.Remove(tmpfile.Name())
		if _, err = tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err = tmpfile.Close(); err != nil {
			t.Fatal(err)
		}

		err = fsys.Rename(oldPath, newPath)
		if expected, actual := true, ErrNotFound(err); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("exists", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fsys := NewVirtualFilesystem()
		testFilesystemExists(fsys, dir, t)
	})

	t.Run("remove", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fsys := NewVirtualFilesystem()
		testFilesystemRemove(fsys, dir, t)
	})

	t.Run("walk", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fsys := NewVirtualFilesystem()
		testFilesystemWalk(fsys, dir, t)
	})

	t.Run("walk with failure", func(t *testing.T) {
		var (
			dir  = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fsys = NewVirtualFilesystem()
			path = filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
		)
		if _, err := fsys.Create(path); err != nil {
			t.Error(err)
		}

		fatal := errors.New("fatal")
		err := fsys.Walk(dir, func(path string, info os.FileInfo, err error) error {
			return fatal
		})

		if expected, actual := fatal, err; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("walk with path mismatch", func(t *testing.T) {
		var (
			fsys = NewVirtualFilesystem()
			dir  string
		)
		for i := 0; i < 10; i++ {
			dir = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			path := filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
			if _, err := fsys.Create(path); err != nil {
				t.Error(err)
			}
		}

		err := fsys.Walk(dir, func(path string, info os.FileInfo, err error) error {
			return nil
		})

		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestVirtualFile(t *testing.T) {
	t.Parallel()

	t.Run("name", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fsys := NewVirtualFilesystem()
		testFileName(fsys, dir, t)
	})

	t.Run("size", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fsys := NewVirtualFilesystem()
		testFileSize(fsys, dir, t)
	})

	t.Run("read and write", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fsys := NewVirtualFilesystem()
		testFileReadWrite(fsys, dir, t)
	})

	t.Run("sync", func(t *testing.T) {
		var (
			fsys     = NewVirtualFilesystem()
			dir      = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fileName = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
			path     = filepath.Join(dir, fileName)
		)
		file, err := fsys.Create(path)
		if err != nil {
			t.Error(err)
		}

		defer file.Close()

		res := file.Sync()
		if expected, actual := true, res == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}
