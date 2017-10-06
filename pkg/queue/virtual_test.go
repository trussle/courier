package queue

import (
	"math/rand"
	"testing"
	"testing/quick"
	"time"

	"github.com/trussle/courier/pkg/uuid"
	"github.com/pkg/errors"
)

func TestVirtualQueue(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Run("enqueue", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewVirtualQueue()
			err := queue.Enqueue(r)
			return err == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("reset returns nil", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewVirtualQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}
			return queue.Reset() == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue returns nil", func(t *testing.T) {
		queue := NewVirtualQueue()
		_, err := queue.Dequeue()
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t, err: %v", expected, actual, err)
		}
	})

	t.Run("dequeue returns nil", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewVirtualQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}
			_, err := queue.Dequeue()
			return err == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue returns segment with valid size", func(t *testing.T) {
		fn := func(r []Record) bool {
			if len(r) == 0 {
				return true
			}

			queue := NewVirtualQueue()
			for _, v := range r {
				if err := queue.Enqueue(v); err != nil {
					t.Fatal(err)
				}
			}
			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}
			return s.Size() == 1
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("multiple dequeues returns segment with valid size", func(t *testing.T) {
		fn := func(r []Record) bool {
			queue := NewVirtualQueue()
			for _, v := range r {
				if err := queue.Enqueue(v); err != nil {
					t.Fatal(err)
				}
			}

			count := 0
			for range r {
				s, err := queue.Dequeue()
				if err != nil {
					t.Fatal(err)
				}

				size := s.Size()
				count += size

				if expected, actual := 1, size; expected != actual {
					t.Errorf("expected: %d, actual: %d", expected, actual)
				}
			}

			return count == len(r)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewVirtualQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}
			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			err = s.Walk(func(a Record) error {
				if expected, actual := r, a; !expected.Equals(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}
				return nil
			})

			return err == nil && s.Size() == 1
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk with error", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewVirtualQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}
			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			err = s.Walk(func(a Record) error {
				return errors.New("bad")
			})

			return err != nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue should return valid id", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewVirtualQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}
			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			if expected, actual := false, s.ID().Zero(); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("commit without records returns nil", func(t *testing.T) {
		s := newVirtualSegment(uuid.MustNew(rnd), make([]Record, 0))
		res, err := s.Commit(make([]uuid.UUID, 0))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 0, res.Successful; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 0, res.Unsuccessful; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("commit changes size without ids", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewVirtualQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}
			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			res, err := s.Commit(make([]uuid.UUID, 0))
			if err != nil {
				t.Fatal(err)
			}

			return s.Size() == 0 && res.Unsuccessful == 1
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("commit changes size with invalid ids", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewVirtualQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}
			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			res, err := s.Commit([]uuid.UUID{
				uuid.MustNew(rnd),
			})
			if err != nil {
				t.Fatal(err)
			}

			return s.Size() == 0 && res.Unsuccessful == 1
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("commit changes size", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewVirtualQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}
			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			err = s.Walk(func(a Record) error {
				if expected, actual := r, a; !expected.Equals(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}

			res, err := s.Commit([]uuid.UUID{
				r.ID,
			})
			if err != nil {
				t.Fatal(err)
			}
			return s.Size() == 0 && res.Successful == 1
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("failed without records returns nil", func(t *testing.T) {
		s := newVirtualSegment(uuid.MustNew(rnd), make([]Record, 0))
		res, err := s.Failed(make([]uuid.UUID, 0))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 0, res.Successful; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 0, res.Unsuccessful; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("failed changes size without walk", func(t *testing.T) {
		fn := func(r Record) bool {
			queue := NewVirtualQueue()
			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}
			s, err := queue.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			res, err := s.Failed(make([]uuid.UUID, 0))
			if err != nil {
				t.Fatal(err)
			}
			return s.Size() == 0 && res.Unsuccessful == 1
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
