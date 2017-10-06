package fs

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNopFilesystem(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		var (
			dir  = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fsys = NewNopFilesystem()
			path = filepath.Join(dir, "tmpfile")
		)

		file, err := fsys.Create(path)
		if err != nil {
			t.Error(err)
		}
		if file == nil {
			t.Error("expected file to exist")
		}
	})

	t.Run("open", func(t *testing.T) {
		var (
			dir      = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fileName = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
			path     = filepath.Join(dir, fileName)
			fsys     = NewNopFilesystem()
		)

		file, err := fsys.Open(path)
		if err != nil {
			t.Error(err)
		}
		if file == nil {
			t.Error("expected file to exist")
		}
	})

	t.Run("rename", func(t *testing.T) {
		var (
			dir       = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fileName0 = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
			fileName1 = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
			path0     = filepath.Join(dir, fileName0)
			path1     = filepath.Join(dir, fileName1)
			fsys      = NewNopFilesystem()
		)

		if err := fsys.Rename(path0, path1); err != nil {
			t.Error(err)
		}
	})

	t.Run("exists", func(t *testing.T) {
		var (
			fsys = NewNopFilesystem()
			dir  = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		)
		if path := filepath.Join(dir, "tmpfile"); fsys.Exists(path) {
			t.Errorf("expected: %q to not exist", path)
		}

		path := filepath.Join(dir, "tmpfile")
		if _, err := fsys.Create(path); err != nil {
			t.Error(err)
		}
		if path := filepath.Join(dir, "tmpfile"); fsys.Exists(path) {
			t.Errorf("expected: %q to not exist", path)
		}
	})

	t.Run("remove", func(t *testing.T) {
		var (
			fsys = NewNopFilesystem()
			dir  = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			path = filepath.Join(dir, "tmpfile")
		)
		if _, err := fsys.Create(path); err != nil {
			t.Error(err)
		}
		if err := fsys.Remove(path); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk", func(t *testing.T) {
		var (
			fsys = NewNopFilesystem()
			dir  = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			path = filepath.Join(dir, "tmpfile")
		)
		if _, err := fsys.Create(path); err != nil {
			t.Error(err)
		}
		if err := fsys.Walk(dir, func(path string, info os.FileInfo, err error) error {
			t.Error("expected: not to be called")
			return err
		}); err != nil {
			t.Error(err)
		}
	})
}

func TestNopFile(t *testing.T) {
	t.Parallel()

	t.Run("name", func(t *testing.T) {
		var (
			fsys = NewNopFilesystem()
			dir  = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			path = filepath.Join(dir, "tmpfile")
		)
		file, err := fsys.Create(path)
		if err != nil {
			t.Error(err)
		}

		if expected, actual := file.Name(), ""; expected != actual {
			t.Errorf("expected: %q, actual: %q", expected, actual)
		}
	})

	t.Run("size", func(t *testing.T) {
		var (
			fsys = NewNopFilesystem()
			dir  = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			path = filepath.Join(dir, "tmpfile")
		)
		file, err := fsys.Create(path)
		if err != nil {
			t.Error(err)
		}

		if expected, actual := file.Size(), int64(0); expected != actual {
			t.Errorf("expected: %q, actual: %q", expected, actual)
		}
	})

	t.Run("read and write", func(t *testing.T) {
		var (
			fsys = NewNopFilesystem()
			dir  = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			path = filepath.Join(dir, "tmpfile")
		)
		file, err := fsys.Create(path)
		if err != nil {
			t.Error(err)
		}

		var (
			n     int
			bytes []byte
		)
		if n, err = file.Read(bytes); err != nil {
			t.Error(err)
		} else if n != 0 {
			t.Errorf("expected: %q to be 0", n)
		}

		content := make([]byte, rand.Intn(1000)+100)
		if _, err = rand.Read(content); err != nil {
			t.Error(err)
		}
		if n, err = file.Write(content); err != nil {
			t.Error(err)
		} else if n == 0 || n != len(content) {
			t.Errorf("expected: %q to be %d", n, len(content))
		}

		if err = file.Close(); err != nil {
			t.Error(err)
		}

		// For some reason, we can't read after a write
		file, err = fsys.Open(path)
		if err != nil {
			t.Error(err)
		}

		defer file.Close()

		contentBytes := make([]byte, len(content))
		if n, err := file.Read(contentBytes); err != nil {
			t.Error(err)
		} else if n == 0 || n != len(content) {
			t.Errorf("expected: %q to be %d", n, len(content))
		}

		if expected, actual := make([]byte, len(content)), contentBytes; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %q, actual: %q", expected, actual)
		}
	})

	t.Run("sync", func(t *testing.T) {
		var (
			fsys = NewNopFilesystem()
			dir  = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			path = filepath.Join(dir, "tmpfile")
		)
		file, err := fsys.Create(path)
		if err != nil {
			t.Error(err)
		}

		var (
			n     int
			bytes []byte
		)
		if n, err = file.Read(bytes); err != nil {
			t.Error(err)
		} else if n != 0 {
			t.Errorf("expected: %q to be 0", n)
		}

		content := make([]byte, rand.Intn(1000)+100)
		if _, err = rand.Read(content); err != nil {
			t.Error(err)
		}
		if n, err = file.Write(content); err != nil {
			t.Error(err)
		} else if n == 0 || n != len(content) {
			t.Errorf("expected: %q to be %d", n, len(content))
		}
		if err = file.Sync(); err != nil {
			t.Error(err)
		}
		if err = file.Close(); err != nil {
			t.Error(err)
		}
	})
}
