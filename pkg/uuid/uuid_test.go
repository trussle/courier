package uuid

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"
	"time"
)

func TestUUID(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Run("new", func(t *testing.T) {
		fn := func() bool {
			id, err := New(rnd)
			if err != nil {
				t.Fatal(err)
			}
			return len(id.String()) == 36
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("must", func(t *testing.T) {
		fn := func() bool {
			id := MustNew(rnd)
			return len(id.String()) == 36
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("string", func(t *testing.T) {
		fn := func() bool {
			id := Empty
			return len(id.String()) == 36
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("length", func(t *testing.T) {
		fn := func(id UUID) bool {
			return len(id.String()) == 36
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("matches", func(t *testing.T) {
		fn := func(id UUID) bool {
			return layout.Match(id.Bytes())
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("matches none", func(t *testing.T) {
		fn := func(a, b UUID) bool {
			return !a.Equals(b)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		fn := func(id UUID) bool {
			return len(id.Bytes()) == 36
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("parse bad", func(t *testing.T) {
		_, err := Parse("bad")

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("parse isomorphic", func(t *testing.T) {
		fn := func(id UUID) bool {
			actual := id.String()
			uuid, err := Parse(actual)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return uuid.String() == actual
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("must parse isomorphic", func(t *testing.T) {
		fn := func(id UUID) bool {
			actual := id.String()
			uuid := MustParse(actual)

			return uuid.String() == actual
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("marshal json", func(t *testing.T) {
		fn := func(id UUID) bool {
			res, err := json.Marshal(id)
			if err != nil {
				t.Error(err)
			}

			return string(res) == fmt.Sprintf("%q", id.String())
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("unmarshal json", func(t *testing.T) {
		fn := func(id UUID) bool {
			data, err := json.Marshal(id)
			if err != nil {
				t.Error(err)
			}

			var res UUID
			err = json.Unmarshal(data, &res)
			if err != nil {
				t.Error(err)
			}

			return res.Equals(id)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("unmarshal invalid json", func(t *testing.T) {
		fn := func(id []byte) bool {
			data, err := json.Marshal(id)
			if err != nil {
				t.Error(err)
			}

			var res UUID
			err = json.Unmarshal(data, &res)
			return err != nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("zero", func(t *testing.T) {
		if expected, actual := true, Empty.Zero(); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("zero string", func(t *testing.T) {
		id, err := Parse(Empty.String())
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := true, id.Zero(); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}
