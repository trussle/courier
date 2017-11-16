package queue

import (
	"testing"
	"testing/quick"

	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/uuid"
)

func TestTransaction(t *testing.T) {
	t.Parallel()

	t.Run("len", func(t *testing.T) {
		txn := NewTransaction()

		if expected, actual := 0, txn.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("push then len", func(t *testing.T) {
		fn := func(id uuid.UUID, rec queueRecord) bool {
			txn := NewTransaction()

			if err := txn.Push(id, rec); err != nil {
				t.Fatal(err)
			}

			if expected, actual := 1, txn.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("push then walk", func(t *testing.T) {
		fn := func(id uuid.UUID, rec queueRecord) bool {
			txn := NewTransaction()

			if err := txn.Push(id, rec); err != nil {
				t.Fatal(err)
			}

			var called bool
			err := txn.Walk(func(a uuid.UUID, b models.Record) error {
				called = true

				if expected, actual := true, id.Equals(a); expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
				if expected, actual := true, rec.Equal(b); expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
				return nil
			})

			return called && err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("push then walk error", func(t *testing.T) {
		fn := func(id uuid.UUID, rec queueRecord) bool {
			txn := NewTransaction()

			if err := txn.Push(id, rec); err != nil {
				t.Fatal(err)
			}

			var called bool
			err := txn.Walk(func(a uuid.UUID, b models.Record) error {
				called = true

				if expected, actual := true, id.Equals(a); expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
				if expected, actual := true, rec.Equal(b); expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
				return errors.New("bad")
			})

			return called && err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("push then flush", func(t *testing.T) {
		fn := func(id uuid.UUID, rec queueRecord) bool {
			txn := NewTransaction()

			if err := txn.Push(id, rec); err != nil {
				t.Fatal(err)
			}

			if expected, actual := 1, txn.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			if err := txn.Flush(); err != nil {
				t.Fatal(err)
			}

			return txn.Len() == 0
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
