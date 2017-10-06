package queue

import (
	"testing"
	"testing/quick"
)

func TestRecords(t *testing.T) {
	t.Parallel()

	t.Run("append", func(t *testing.T) {
		rec := new(Records)
		rec.Append(Record{})

		if expected, actual := 1, rec.Len(); expected != actual {
			t.Fatalf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("append multiple times", func(t *testing.T) {
		fn := func(r []Record) bool {
			rec := new(Records)
			for _, v := range r {
				rec.Append(v)
			}

			return rec.Len() == len(r)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
