package store

import (
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/trussle/harness/generators"
)

func TestVirtual(t *testing.T) {
	t.Parallel()

	t.Run("add", func(t *testing.T) {
		fn := func(a []string) bool {
			store := newVirtualStore(1)
			return store.Add(a) == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("intersection with no values", func(t *testing.T) {
		fn := func(a []string) bool {
			store := newVirtualStore(len(a))
			union, difference, err := store.Intersection(a)
			if expected, actual := 0, len(union); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return match(a, difference)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("intersection", func(t *testing.T) {
		fn := func(a generators.ASCIISlice) bool {
			idents := a.Slice()
			store := newVirtualStore(len(idents))
			if err := store.Add(idents); err != nil {
				t.Fatal(err)
			}

			union, difference, err := store.Intersection(idents)
			if expected, actual := 0, len(unique(difference)); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			return match(idents, union)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("intersection overlap", func(t *testing.T) {
		fn := func(a, b []string) bool {
			store := newVirtualStore(len(a) + len(b))
			if err := store.Add(a); err != nil {
				t.Fatal(err)
			}
			if err := store.Add(b); err != nil {
				t.Fatal(err)
			}

			union, difference, err := store.Intersection(a)
			if expected, actual := 0, len(unique(difference)); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			return match(a, union)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("intersection capped", func(t *testing.T) {
		idents := []string{
			"a", "b", "c", "d", "e",
		}

		store := newVirtualStore(3)
		if err := store.Add(idents); err != nil {
			t.Fatal(err)
		}

		union, difference, err := store.Intersection(idents)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		cap := 2
		if expected, actual := idents[cap:], union; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := idents[:cap], difference; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("intersection with duplicates", func(t *testing.T) {
		idents := []string{
			"a", "a", "c", "a", "e",
		}

		store := newVirtualStore(3)
		if err := store.Add(idents); err != nil {
			t.Fatal(err)
		}

		union, difference, err := store.Intersection(idents)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		if expected, actual := []string{"a", "c", "e"}, union; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := []string{}, difference; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func match(a, b []string) bool {
	want := unique(a)
	got := b

	sort.Strings(want)
	sort.Strings(got)

	return reflect.DeepEqual(want, got)
}
