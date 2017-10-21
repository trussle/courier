package lru_test

import (
	"reflect"
	"testing"

	"github.com/trussle/courier/pkg/lru"
)

func TestLRU_Add(t *testing.T) {
	t.Parallel()

	t.Run("adding with eviction", func(t *testing.T) {
		evictted := 0
		onEviction := func(k lru.Key, v lru.Value) {
			if expected, actual := 1, k; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			evictted += 1
		}

		l := lru.NewLRU(1, onEviction)

		if expected, actual := false, l.Add(1, 1); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
		if expected, actual := true, l.Add(2, 2); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
		if expected, actual := 1, evictted; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 1, l.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		values := []lru.KeyValue{
			lru.KeyValue{2, 2},
		}
		if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("adding sorts keys", func(t *testing.T) {
		onEviction := func(k lru.Key, v lru.Value) {
			t.Fatal("failed if called")
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		l.Add(1, 4)

		values := []lru.KeyValue{
			lru.KeyValue{2, 2},
			lru.KeyValue{3, 3},
			lru.KeyValue{1, 4},
		}
		if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestLRU_Get(t *testing.T) {
	t.Parallel()

	t.Run("get", func(t *testing.T) {
		onEviction := func(k lru.Key, v lru.Value) {
			t.Fatal("failed if called")
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		value, ok := l.Get(1)

		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
		if expected, actual := 1, value; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("get sorts keys", func(t *testing.T) {
		onEviction := func(k lru.Key, v lru.Value) {
			t.Fatal("failed if called")
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		l.Get(1)

		values := []lru.KeyValue{
			lru.KeyValue{2, 2},
			lru.KeyValue{3, 3},
			lru.KeyValue{1, 1},
		}
		if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestLRU_Peek(t *testing.T) {
	t.Parallel()

	t.Run("peek", func(t *testing.T) {
		onEviction := func(k lru.Key, v lru.Value) {
			t.Fatal("failed if called")
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		value, ok := l.Peek(1)

		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
		if expected, actual := 1, value; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("peek does not sorts keys", func(t *testing.T) {
		onEviction := func(k lru.Key, v lru.Value) {
			t.Fatal("failed if called")
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		l.Peek(1)

		values := []lru.KeyValue{
			lru.KeyValue{1, 1},
			lru.KeyValue{2, 2},
			lru.KeyValue{3, 3},
		}
		if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestLRU_Contains(t *testing.T) {
	t.Parallel()

	t.Run("contains", func(t *testing.T) {
		onEviction := func(k lru.Key, v lru.Value) {
			t.Fatal("failed if called")
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		ok := l.Contains(2)

		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
	t.Run("does not contains", func(t *testing.T) {
		onEviction := func(k lru.Key, v lru.Value) {
			t.Fatal("failed if called")
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		ok := l.Contains(6)

		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestLRU_Remove(t *testing.T) {
	t.Parallel()

	t.Run("removes key value pair", func(t *testing.T) {
		evictted := 0
		onEviction := func(k lru.Key, v lru.Value) {
			if expected, actual := 1, k; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			evictted += 1
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		l.Remove(1)

		if expected, actual := 1, evictted; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		values := []lru.KeyValue{
			lru.KeyValue{2, 2},
			lru.KeyValue{3, 3},
		}
		if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestLRU_Pop(t *testing.T) {
	t.Parallel()

	t.Run("pop on empty", func(t *testing.T) {
		onEviction := func(k lru.Key, v lru.Value) {
			t.Fatal("failed if called")
		}

		l := lru.NewLRU(3, onEviction)

		_, _, ok := l.Pop()

		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("pop", func(t *testing.T) {
		evictted := 0
		onEviction := func(k lru.Key, v lru.Value) {
			if expected, actual := 1, k; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			evictted += 1
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		key, value, ok := l.Pop()

		if expected, actual := 1, evictted; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
		if expected, actual := 1, key; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 1, value; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("pop results", func(t *testing.T) {
		evictted := 0
		onEviction := func(k lru.Key, v lru.Value) {
			if expected, actual := 1, k; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			evictted += 1
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		l.Pop()

		if expected, actual := 1, evictted; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		values := []lru.KeyValue{
			lru.KeyValue{2, 2},
			lru.KeyValue{3, 3},
		}
		if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestLRU_Purge(t *testing.T) {
	t.Parallel()

	t.Run("purge", func(t *testing.T) {
		evictted := 0
		onEviction := func(k lru.Key, v lru.Value) {
			evictted += 1
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		values := []lru.KeyValue{
			lru.KeyValue{1, 1},
			lru.KeyValue{2, 2},
			lru.KeyValue{3, 3},
		}
		if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		l.Purge()

		if expected, actual := 3, evictted; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		values = []lru.KeyValue{}
		if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestLRU_Keys(t *testing.T) {
	t.Parallel()

	t.Run("keys", func(t *testing.T) {
		onEviction := func(k lru.Key, v lru.Value) {
			t.Fatal("failed if called")
		}

		l := lru.NewLRU(3, onEviction)

		l.Add(1, 1)
		l.Add(2, 2)
		l.Add(3, 3)

		got := l.Keys()

		values := []lru.Key{
			lru.Key(1),
			lru.Key(2),
			lru.Key(3),
		}
		if expected, actual := values, got; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}
