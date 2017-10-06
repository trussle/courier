package queue

import (
	"testing"
	"testing/quick"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

func TestBuildingQueue(t *testing.T) {
	t.Parallel()

	t.Run("build", func(t *testing.T) {
		fn := func(name string) bool {
			config, err := Build(
				With(name),
				WithConfig(nil),
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

	t.Run("nop", func(t *testing.T) {
		config, err := Build(
			With("nop"),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = New(config, log.NewNopLogger())
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

		_, err = New(config, log.NewNopLogger())
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestNoSegmentsAvailable(t *testing.T) {
	t.Parallel()

	t.Run("source", func(t *testing.T) {
		fn := func(source string) bool {
			err := errNoSegmentsAvailable{errors.New(source)}

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
			err := errNoSegmentsAvailable{errors.New(source)}

			if expected, actual := true, err.NoSegmentsAvailable(); expected != actual {
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
			err := errNoSegmentsAvailable{errors.New(source)}

			if expected, actual := true, ErrNoSegmentsAvailable(err); expected != actual {
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

			if expected, actual := false, ErrNoSegmentsAvailable(err); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
