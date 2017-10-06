package fs

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/pkg/errors"
)

func TestBuildingFilesystem(t *testing.T) {
	t.Parallel()

	t.Run("build", func(t *testing.T) {
		fn := func(name string) bool {
			config, err := Build(
				With(name),
				WithMMAP(true),
			)
			if err != nil {
				t.Fatal(err)
			}

			if expected, actual := name, config.name; expected != actual {
				t.Errorf("expected: %s, actual: %s", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid build", func(t *testing.T) {
		_, err := Build(
			func(config *Config) error {
				return errors.Errorf("bad")
			},
		)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("local", func(t *testing.T) {
		config, err := Build(
			With("local"),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = New(config)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("virtual", func(t *testing.T) {
		config, err := Build(
			With("virtual"),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = New(config)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("nop", func(t *testing.T) {
		config, err := Build(
			With("nop"),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = New(config)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		config, err := Build(
			With("invalid"),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = New(config)
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestNotFound(t *testing.T) {
	t.Parallel()

	t.Run("source", func(t *testing.T) {
		fn := func(source string) bool {
			err := errNotFound{errors.New(source)}

			if expected, actual := source, err.Error(); expected != actual {
				t.Errorf("expected: %q, actual: %q", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		fn := func(source string) bool {
			err := errNotFound{errors.New(source)}

			if expected, actual := true, err.NotFound(); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("valid", func(t *testing.T) {
		fn := func(source string) bool {
			err := errNotFound{errors.New(source)}

			if expected, actual := true, ErrNotFound(err); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		fn := func(source string) bool {
			err := errors.New(source)

			if expected, actual := false, ErrNotFound(err); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func testFilesystemCreate(fsys Filesystem, dir string, t *testing.T) {
	path := filepath.Join(dir, "tmpfile")
	file, err := fsys.Create(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	if !fsys.Exists(path) {
		t.Errorf("expected: %q to exist", path)
	}
	if expected, actual := int64(0), file.Size(); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}

func testFilesystemOpen(fsys Filesystem, dir string, t *testing.T) {
	path := filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
	tmpfile, err := fsys.Create(path)
	if err != nil {
		t.Error(err)
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

	file, err := fsys.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	buf := make([]byte, len(content))
	if _, err := io.ReadFull(file, buf); err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(content, buf) {
		t.Errorf("expected: %v, actual: %v", content, buf)
	}
}

func testFilesystemRename(fsys Filesystem, dir string, t *testing.T) {
	path := filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
	tmpfile, err := fsys.Create(path)
	if err != nil {
		t.Error(err)
	}

	content := make([]byte, rand.Intn(1000)+100)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	if _, err := tmpfile.Write(content); err != nil {
		t.Fatal(err)
	}

	defer fsys.Remove(tmpfile.Name())
	if _, err := tmpfile.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	var (
		oldPath = tmpfile.Name()
		newPath = fmt.Sprintf("%s-new", tmpfile.Name())
	)
	if err := fsys.Rename(oldPath, newPath); err != nil {
		t.Error(err)
	}

	if fsys.Exists(oldPath) {
		t.Errorf("expected: %q to not exist", newPath)
	}

	if !fsys.Exists(newPath) {
		t.Errorf("expected: %q to exist", newPath)
	}
}

func testFilesystemExists(fsys Filesystem, dir string, t *testing.T) {
	if path := filepath.Join(dir, "tmpfile"); fsys.Exists(path) {
		t.Errorf("expected: %q to not exist", path)
	}

	// exists is run in all the following
	testFilesystemOpen(fsys, dir, t)
	testFilesystemCreate(fsys, dir, t)
	testFilesystemRename(fsys, dir, t)
	testFilesystemRemove(fsys, dir, t)
}

func testFilesystemRemove(fsys Filesystem, dir string, t *testing.T) {
	path := filepath.Join(dir, "tmpfile")
	file, err := fsys.Create(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	if !fsys.Exists(path) {
		t.Errorf("expected: %q to exist", path)
	}

	if err := fsys.Remove(path); err != nil {
		t.Errorf("expected: %q to not exist", path)
	}
}

func testFilesystemWalk(fsys Filesystem, dir string, t *testing.T) {
	contains := func(paths []string, path string) bool {
		for _, v := range paths {
			if v == path {
				return true
			}
		}
		return false
	}
	paths := make([]string, rand.Intn(100)+1)
	for k := range paths {
		path := filepath.Join(dir, fmt.Sprintf("tmpfile-%d", k))
		file, err := fsys.Create(path)
		if err != nil {
			t.Error(err)
		}

		defer file.Close()

		if !fsys.Exists(file.Name()) {
			t.Errorf("expected: %q to exist", file.Name())
		}

		paths[k] = path
	}

	if err := fsys.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info == nil {
			t.Errorf("expected: %q file info to exist", path)
		}

		if info.IsDir() {
			return nil
		}

		filepath := filepath.Join(dir, info.Name())
		if !contains(paths, filepath) {
			t.Errorf("expected: %q to exist", filepath)
		}

		if expected, actual := int64(0), info.Size(); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		return err
	}); err != nil {
		t.Error(err)
	}
}

func testFileName(fsys Filesystem, dir string, t *testing.T) {
	var (
		fileName = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
		path     = filepath.Join(dir, fileName)
	)
	file, err := fsys.Create(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	if !fsys.Exists(path) {
		t.Errorf("expected: %q to exist", path)
	}

	if expected, actual := path, file.Name(); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}

func testFileSize(fsys Filesystem, dir string, t *testing.T) {
	var (
		fileName = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
		path     = filepath.Join(dir, fileName)
	)
	file, err := fsys.Create(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	if !fsys.Exists(path) {
		t.Errorf("expected: %q to exist", path)
	}
	if expected, actual := int64(0), file.Size(); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}

	content := make([]byte, rand.Intn(1000)+100)
	if _, err := rand.Read(content); err != nil {
		t.Error(err)
	}
	if _, err := file.Write(content); err != nil {
		t.Error(err)
	}

	if expected, actual := file.Size(), int64(len(content)); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}

func testFileReadWrite(fsys Filesystem, dir string, t *testing.T) {
	var (
		fileName = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
		path     = filepath.Join(dir, fileName)
	)
	file, err := fsys.Create(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	if !fsys.Exists(path) {
		t.Errorf("expected: %q to exist", path)
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

	// For some reason, we can't read after a write
	file, err = fsys.Open(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	contentBytes, err := ioutil.ReadAll(file)
	if err != nil {
		t.Error(err)
	}
	if expected, actual := content, contentBytes; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}
