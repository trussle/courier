package queue

import (
	"testing"
	"testing/quick"

	"github.com/trussle/courier/pkg/uuid"
	"github.com/pkg/errors"
)

func TestNopQueue(t *testing.T) {
	t.Parallel()

	t.Run("enqueue", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewNopQueue()
			err := queue.Enqueue(r)
			return err == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewNopQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}

			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			return s.Size() == 0
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue empty queue", func(t *testing.T) {
		queue := NewNopQueue()
		_, err := queue.Dequeue()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("dequeue then commit segment returns nil", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewNopQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}

			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			_, err = s.Commit(make([]uuid.UUID, 0))
			return err == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue then fail segment returns nil", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewNopQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}

			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			_, err = s.Failed(make([]uuid.UUID, 0))
			return err == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue then walk returns nil", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewNopQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}

			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			err = s.Walk(func(r Record) error {
				t.Fatal(errors.New("failed if called"))
				return nil
			})

			return err == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("reset", func(t *testing.T) {
		queue := NewNopQueue()
		err := queue.Reset()

		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}
