package fifo_test

import (
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/trussle/courier/pkg/store/fifo"
)

func TestFIFO_Add(t *testing.T) {
	t.Parallel()

	t.Run("adding with eviction", func(t *testing.T) {
		fn := func(id0, id1 ASCII) bool {
			onEviction := func(reason fifo.EvictionReason, k string) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(3, onEviction)

			if expected, actual := true, l.Add(id0.String()); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := true, l.Add(id1.String()); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 2, l.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values := []string{
				id0.String(),
				id1.String(),
			}
			if expected, actual := values, l.Keys(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("adding sorts keys", func(t *testing.T) {
		fn := func(id0, id1, id2 ASCII) bool {
			onEviction := func(reason fifo.EvictionReason, k string) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(4, onEviction)

			l.Add(id0.String())
			l.Add(id1.String())
			l.Add(id2.String())

			l.Add(id0.String())

			values := []string{
				id0.String(),
				id1.String(),
				id2.String(),
				id0.String(),
			}
			if expected, actual := values, l.Keys(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("adding with size", func(t *testing.T) {
		fn := func(id0, id1, id2, id3 ASCII) bool {
			onEviction := func(reason fifo.EvictionReason, k string) {
				if expected, actual := id0.String(), k; expected != actual {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}
			}

			l := fifo.NewFIFO(3, onEviction)

			l.Add(id0.String())
			l.Add(id1.String())
			l.Add(id2.String())

			l.Add(id3.String())

			values := []string{
				id1.String(),
				id2.String(),
				id3.String(),
			}
			if expected, actual := values, l.Keys(); !reflect.DeepEqual(expected, actual) {
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
		fn := func(id0, id1, id2 ASCII) bool {
			onEviction := func(reason fifo.EvictionReason, k string) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(3, onEviction)

			l.Add(id0.String())
			l.Add(id1.String())
			l.Add(id2.String())

			ok := l.Contains(id1.String())

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
		fn := func(id0, id1, id2, id3 ASCII) bool {
			onEviction := func(reason fifo.EvictionReason, k string) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(3, onEviction)

			l.Add(id0.String())
			l.Add(id1.String())
			l.Add(id2.String())

			ok := l.Contains(id3.String())

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
		fn := func(id0, id1, id2 ASCII) bool {
			evictted := 0
			onEviction := func(reason fifo.EvictionReason, k string) {
				if expected, actual := id0.String(), k; expected != actual {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				evictted += 1
			}

			l := fifo.NewFIFO(3, onEviction)

			l.Add(id0.String())
			l.Add(id1.String())
			l.Add(id2.String())

			l.Remove(id0.String())

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values := []string{
				id1.String(),
				id2.String(),
			}
			if expected, actual := values, l.Keys(); !reflect.DeepEqual(expected, actual) {
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
		onEviction := func(reason fifo.EvictionReason, k string) {
			t.Fatal("failed if called")
		}

		l := fifo.NewFIFO(3, onEviction)

		_, ok := l.Pop()

		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("pop", func(t *testing.T) {
		fn := func(id0, id1, id2 ASCII) bool {
			evictted := 0
			onEviction := func(reason fifo.EvictionReason, k string) {
				if expected, actual := id0.String(), k; expected != actual {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				evictted += 1
			}

			l := fifo.NewFIFO(3, onEviction)

			l.Add(id0.String())
			l.Add(id1.String())
			l.Add(id2.String())

			key, ok := l.Pop()

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			if expected, actual := true, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := id0.String(), key; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("pop results", func(t *testing.T) {
		fn := func(id0, id1, id2 ASCII) bool {
			evictted := 0
			onEviction := func(reason fifo.EvictionReason, k string) {
				if expected, actual := id0.String(), k; expected != actual {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				evictted += 1
			}

			l := fifo.NewFIFO(3, onEviction)

			l.Add(id0.String())
			l.Add(id1.String())
			l.Add(id2.String())

			l.Pop()

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values := []string{
				id1.String(),
				id2.String(),
			}
			if expected, actual := values, l.Keys(); !reflect.DeepEqual(expected, actual) {
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
		fn := func(id0, id1, id2 ASCII) bool {
			evictted := 0
			onEviction := func(reason fifo.EvictionReason, k string) {
				evictted += 1
			}

			l := fifo.NewFIFO(3, onEviction)

			l.Add(id0.String())
			l.Add(id1.String())
			l.Add(id2.String())

			values := []string{
				id0.String(),
				id1.String(),
				id2.String(),
			}
			if expected, actual := values, l.Keys(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			l.Purge()

			if expected, actual := 3, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			values = []string{}
			if expected, actual := values, l.Keys(); !reflect.DeepEqual(expected, actual) {
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
		fn := func(id0, id1, id2 ASCII) bool {
			onEviction := func(reason fifo.EvictionReason, k string) {
				t.Fatal("failed if called")
			}

			l := fifo.NewFIFO(3, onEviction)

			l.Add(id0.String())
			l.Add(id1.String())
			l.Add(id2.String())

			got := l.Keys()

			values := []string{
				id0.String(),
				id1.String(),
				id2.String(),
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

// ASCII creates a series of tags that are ascii compliant.
type ASCII []byte

// Generate allows ASCII to be used within quickcheck scenarios.
func (ASCII) Generate(r *rand.Rand, size int) reflect.Value {
	var (
		chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		res   = make([]byte, size+1)
	)

	for k := range res {
		res[k] = byte(chars[r.Intn(len(chars)-1)])
	}

	return reflect.ValueOf(res)
}

func (a ASCII) Slice() []byte {
	return a
}

func (a ASCII) String() string {
	return string(a)
}
