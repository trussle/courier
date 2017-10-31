package fifo_test

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/trussle/courier/pkg/consumer/fifo"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

func TestFIFO_Add(t *testing.T) {
	t.Parallel()

	t.Run("adding with eviction", func(t *testing.T) {
		fn := func(id0, id1 uuid.UUID, rec0, rec1 TestRecord) bool {
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(onEviction)

			if expected, actual := true, l.Add(id0, rec0); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := true, l.Add(id1, rec1); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 2, l.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values := []fifo.KeyValue{
				fifo.KeyValue{id0, rec0},
				fifo.KeyValue{id1, rec1},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("adding sorts keys", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2, rec3 TestRecord) bool {
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			l.Add(id0, rec3)

			values := []fifo.KeyValue{
				fifo.KeyValue{id0, rec0},
				fifo.KeyValue{id1, rec1},
				fifo.KeyValue{id2, rec2},
				fifo.KeyValue{id0, rec3},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFIFO_Get(t *testing.T) {
	t.Parallel()

	t.Run("get", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			value, ok := l.Get(id0)

			if expected, actual := true, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := rec0, value; !expected.Equal(actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFIFO_Contains(t *testing.T) {
	t.Parallel()

	t.Run("contains", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			ok := l.Contains(id1)

			if expected, actual := true, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("does not contains", func(t *testing.T) {
		fn := func(id0, id1, id2, id3 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			ok := l.Contains(id3)

			if expected, actual := false, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFIFO_Remove(t *testing.T) {
	t.Parallel()

	t.Run("removes key value pair", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			evictted := 0
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				if expected, actual := id0, k; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				evictted += 1
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			l.Remove(id0)

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values := []fifo.KeyValue{
				fifo.KeyValue{id1, rec1},
				fifo.KeyValue{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFIFO_Pop(t *testing.T) {
	t.Parallel()

	t.Run("pop on empty", func(t *testing.T) {
		onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
			t.Fatal("failed if called")
		}

		l := fifo.NewFIFO(onEviction)

		_, _, ok := l.Pop()

		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("pop", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			evictted := 0
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				if expected, actual := id0, k; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				evictted += 1
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			key, value, ok := l.Pop()

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			if expected, actual := true, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := id0, key; !expected.Equal(actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := rec0, value; !expected.Equal(actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("pop results", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			evictted := 0
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				if expected, actual := id0, k; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				evictted += 1
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			l.Pop()

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values := []fifo.KeyValue{
				fifo.KeyValue{id1, rec1},
				fifo.KeyValue{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFIFO_Purge(t *testing.T) {
	t.Parallel()

	t.Run("purge", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			evictted := 0
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				evictted += 1
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			values := []fifo.KeyValue{
				fifo.KeyValue{id0, rec0},
				fifo.KeyValue{id1, rec1},
				fifo.KeyValue{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			l.Purge()

			if expected, actual := 3, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			values = []fifo.KeyValue{}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFIFO_Keys(t *testing.T) {
	t.Parallel()

	t.Run("keys", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			got := l.Keys()

			values := []uuid.UUID{
				id0,
				id1,
				id2,
			}
			if expected, actual := values, got; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("keys after get", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			l.Get(id0)

			got := l.Keys()

			values := []uuid.UUID{
				id0,
				id1,
				id2,
			}
			if expected, actual := values, got; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFIFO_Dequeue(t *testing.T) {
	t.Parallel()

	t.Run("dequeue", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			evictted := 0
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				evictted += 1
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			values := []fifo.KeyValue{
				fifo.KeyValue{id0, rec0},
				fifo.KeyValue{id1, rec1},
				fifo.KeyValue{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			got, err := l.Dequeue(func(key uuid.UUID, value models.Record) error {
				return nil
			})
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if expected, actual := 3, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := values, got; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			values = []fifo.KeyValue{}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue with error", func(t *testing.T) {
		fn := func(id0, id1, id2 uuid.UUID, rec0, rec1, rec2 TestRecord) bool {
			evictted := 0
			onEviction := func(reason fifo.EvictionReason, k uuid.UUID, v models.Record) {
				evictted += 1
			}

			l := fifo.NewFIFO(onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			values := []fifo.KeyValue{
				fifo.KeyValue{id0, rec0},
				fifo.KeyValue{id1, rec1},
				fifo.KeyValue{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			got, err := l.Dequeue(func(key uuid.UUID, value models.Record) error {
				if key.Equal(id1) {
					return errors.New("bad")
				}
				return nil
			})
			if expected, actual := false, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values = []fifo.KeyValue{
				fifo.KeyValue{id0, rec0},
			}
			if expected, actual := values, got; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			values = []fifo.KeyValue{
				fifo.KeyValue{id1, rec1},
				fifo.KeyValue{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

type TestRecord struct {
	id        uuid.UUID
	messageID string
	receipt   models.Receipt
	body      []byte
	timestamp time.Time
}

func (t TestRecord) ID() uuid.UUID           { return t.id }
func (t TestRecord) Body() []byte            { return t.body }
func (t TestRecord) RecordID() string        { return t.messageID }
func (t TestRecord) Receipt() models.Receipt { return t.receipt }

func (t TestRecord) Commit(txn models.Transaction) error {
	return txn.Push(t.id, t)
}

func (t TestRecord) Failed(txn models.Transaction) error {
	return txn.Push(t.id, t)
}

// Equal checks the equality of records against each other
func (t TestRecord) Equal(other models.Record) bool {
	return t.ID().Equal(other.ID()) &&
		reflect.DeepEqual(t.Body(), other.Body())
}

// Generate allows UUID to be used within quickcheck scenarios.
func (TestRecord) Generate(r *rand.Rand, size int) reflect.Value {
	rec, err := generate(r)
	if err != nil {
		panic(err)
	}
	return reflect.ValueOf(rec)
}

func generate(rnd *rand.Rand) (rec TestRecord, err error) {
	if rec.id, err = uuid.New(rnd); err != nil {
		return
	}

	// MessageID generation
	{
		dst := make([]byte, rnd.Intn(10)+20)
		if _, err = rnd.Read(dst); err != nil {
			return
		}
		rec.messageID = string(dst)
	}

	// Receipt generation
	{
		dst := make([]byte, rnd.Intn(10)+24)
		if _, err = rnd.Read(dst); err != nil {
			return
		}
		rec.receipt = models.Receipt(string(dst))
	}

	// Body generation
	{
		dst := make([]byte, rnd.Intn(10)+48)
		if _, err = rnd.Read(dst); err != nil {
			return
		}
		rec.body = dst
	}

	// Timestamp generation
	rec.timestamp = time.Now().Round(time.Millisecond)

	return
}
