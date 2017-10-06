package stream

import (
	"testing"
	"testing/quick"
	"time"

	"github.com/trussle/courier/pkg/fs"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

func TestBuildingQueue(t *testing.T) {
	t.Parallel()

	t.Run("build", func(t *testing.T) {
		fn := func(name string) bool {
			config, err := Build(
				With(name),
				WithTargetAge(time.Second),
				WithTargetSize(1),
				WithFilesystem(fs.NewNopFilesystem()),
				WithRootDir("/root"),
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

	t.Run("virtual", func(t *testing.T) {
		config, err := Build(
			With("virtual"),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = New(config, log.NewNopLogger())
		if err != nil {
			t.Error(err)
		}
	})
}
