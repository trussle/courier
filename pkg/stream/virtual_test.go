package stream

import (
	"errors"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/queue/mocks"
	"github.com/trussle/courier/pkg/uuid"
)

func TestVirtualStream(t *testing.T) {
	t.Parallel()

	config, err := Build(
		With("virtual"),
		WithTargetAge(time.Second),
		WithTargetSize(1),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("len returns zero for empty stream", func(t *testing.T) {
		stream, err := New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, stream.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("capacity returns zero for empty stream", func(t *testing.T) {
		stream, err := New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}
		if expected, actual := false, stream.Capacity(); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("activeSince should be zero for empty stream", func(t *testing.T) {
		stream := newVirtualStream(1, time.Second)
		if expected, actual := true, stream.activeSince.IsZero(); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("reset sets len to zero", func(t *testing.T) {
		stream, err := New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}
		stream.Reset()
		if expected, actual := 0, stream.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("append does updates len to correct value", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		segment := mocks.NewMockSegment(ctrl)

		stream := newVirtualStream(1, time.Second)
		if err := stream.Append(segment); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, stream.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := false, stream.activeSince.IsZero(); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("walk on empty stream yields no call", func(t *testing.T) {
		stream, err := New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}
		err = stream.Walk(func(queue.Segment) error {
			t.Fatal(errors.New("failed if called"))
			return nil
		})
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("walk on stream yields a segment", func(t *testing.T) {
		fn := func(record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().Walk(Walk(record)).Return(nil)

			stream := newVirtualStream(1, time.Second)
			if err := stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			var called bool
			err := stream.Walk(func(s queue.Segment) error {
				return s.Walk(func(r queue.Record) error {
					called = true
					if expected, actual := record, r; !expected.Equals(actual) {
						t.Errorf("expected: %v, actual: %v", expected, actual)
					}
					return nil
				})
			})
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 1, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := true, called; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk on stream yields a failure", func(t *testing.T) {
		fn := func(record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			segment := mocks.NewMockSegment(ctrl)

			stream := newVirtualStream(1, time.Second)
			if err := stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			err := stream.Walk(func(s queue.Segment) error {
				return errors.New("bad")
			})
			if expected, actual := true, err != nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 1, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk on stream then commit", func(t *testing.T) {
		fn := func(id uuid.UUID, record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ids := []uuid.UUID{
				record.ID,
			}

			query := NewQuery()
			query.Set(id, ids)

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(id)
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(2)
			segment.EXPECT().Commit(CompareUUIDs(ids)).Return(queue.Result{}, nil)

			stream := newVirtualStream(1, time.Second)
			if err := stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			err := stream.Walk(func(s queue.Segment) error {
				return s.Walk(func(r queue.Record) error {
					if expected, actual := record, r; !expected.Equals(actual) {
						t.Errorf("expected: %v, actual: %v", expected, actual)
					}
					return nil
				})

			})
			if err != nil {
				t.Fatal(err)
			}

			err = stream.Commit(query)
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 0, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk on stream then commit with miss ids", func(t *testing.T) {
		fn := func(id0, id1 uuid.UUID, record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ids := []uuid.UUID{
				record.ID,
			}

			query := NewQuery()
			query.Set(id1, ids)

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(id0)
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(2)
			segment.EXPECT().Size().Return(1)

			stream := newVirtualStream(1, time.Second)
			if err := stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			err := stream.Walk(func(s queue.Segment) error {
				return s.Walk(func(r queue.Record) error {
					if expected, actual := record, r; !expected.Equals(actual) {
						t.Errorf("expected: %v, actual: %v", expected, actual)
					}
					return nil
				})

			})
			if err != nil {
				t.Fatal(err)
			}

			err = stream.Commit(query)
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 1, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk on stream then commit with query all", func(t *testing.T) {
		fn := func(id0, id1 uuid.UUID, record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ids := []uuid.UUID{
				record.ID,
			}

			query := NewQuery()
			query.Set(id0, ids)
			query.Set(id1, ids)

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(2)
			segment.EXPECT().Commit(CompareUUIDs(ids)).Return(queue.Result{}, nil)

			stream := newVirtualStream(1, time.Second)
			if err := stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			err := stream.Walk(func(s queue.Segment) error {
				return s.Walk(func(r queue.Record) error {
					if expected, actual := record, r; !expected.Equals(actual) {
						t.Errorf("expected: %v, actual: %v", expected, actual)
					}
					return nil
				})

			})
			if err != nil {
				t.Fatal(err)
			}

			err = stream.Commit(All())
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 0, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk on stream then failed", func(t *testing.T) {
		fn := func(id uuid.UUID, record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ids := []uuid.UUID{
				record.ID,
			}

			query := NewQuery()
			query.Set(id, ids)

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(id)
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(2)
			segment.EXPECT().Failed(CompareUUIDs(ids)).Return(queue.Result{}, nil)

			stream := newVirtualStream(1, time.Second)
			if err := stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			err := stream.Walk(func(s queue.Segment) error {
				return s.Walk(func(r queue.Record) error {
					if expected, actual := record, r; !expected.Equals(actual) {
						t.Errorf("expected: %v, actual: %v", expected, actual)
					}
					return nil
				})

			})
			if err != nil {
				t.Fatal(err)
			}

			err = stream.Failed(query)
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 0, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk on stream then failed with miss ids", func(t *testing.T) {
		fn := func(id0, id1 uuid.UUID, record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ids := []uuid.UUID{
				record.ID,
			}

			query := NewQuery()
			query.Set(id1, ids)

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().ID().Return(id0)
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(2)
			segment.EXPECT().Size().Return(1)

			stream := newVirtualStream(1, time.Second)
			if err := stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			err := stream.Walk(func(s queue.Segment) error {
				return s.Walk(func(r queue.Record) error {
					if expected, actual := record, r; !expected.Equals(actual) {
						t.Errorf("expected: %v, actual: %v", expected, actual)
					}
					return nil
				})

			})
			if err != nil {
				t.Fatal(err)
			}

			err = stream.Failed(query)
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 1, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk on stream then failed with query all", func(t *testing.T) {
		fn := func(id0, id1 uuid.UUID, record queue.Record) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ids := []uuid.UUID{
				record.ID,
			}

			query := NewQuery()
			query.Set(id0, ids)
			query.Set(id1, ids)

			segment := mocks.NewMockSegment(ctrl)
			segment.EXPECT().Walk(Walk(record)).Return(nil).Times(2)
			segment.EXPECT().Failed(CompareUUIDs(ids)).Return(queue.Result{}, nil)

			stream := newVirtualStream(1, time.Second)
			if err := stream.Append(segment); err != nil {
				t.Fatal(err)
			}

			err := stream.Walk(func(s queue.Segment) error {
				return s.Walk(func(r queue.Record) error {
					if expected, actual := record, r; !expected.Equals(actual) {
						t.Errorf("expected: %v, actual: %v", expected, actual)
					}
					return nil
				})

			})
			if err != nil {
				t.Fatal(err)
			}

			err = stream.Failed(All())
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 0, len(stream.active); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

type walkMatcher struct {
	record queue.Record
}

func (m walkMatcher) Matches(x interface{}) bool {
	if fn, ok := x.(func(queue.Record) error); ok {
		return fn(m.record) == nil
	}
	return false
}

func (walkMatcher) String() string {
	return "is walk"
}

func Walk(record queue.Record) gomock.Matcher { return walkMatcher{record} }

type uuidMatcher struct {
	id uuid.UUID
}

func (m uuidMatcher) Matches(x interface{}) bool {
	if id, ok := x.(uuid.UUID); ok {
		return id.Equals(m.id)
	}
	return false
}

func (uuidMatcher) String() string {
	return "is uuid"
}

func CompareUUID(id uuid.UUID) gomock.Matcher { return uuidMatcher{id} }

type uuidsMatcher struct {
	ids []uuid.UUID
}

func (m uuidsMatcher) Matches(x interface{}) bool {
	if ids, ok := x.([]uuid.UUID); ok {
		return reflect.DeepEqual(m.ids, ids)
	}
	return false
}

func (uuidsMatcher) String() string {
	return "is []uuid"
}

func CompareUUIDs(ids []uuid.UUID) gomock.Matcher { return uuidsMatcher{ids} }

type queryMatcher struct {
	query *Query
}

func (m queryMatcher) Matches(x interface{}) bool {
	if t, ok := x.(*Query); ok {
		return reflect.DeepEqual(t.segments, m.query.segments)
	}
	return false
}

func (queryMatcher) String() string {
	return "is query"
}

func CompareQuery(t *Query) gomock.Matcher { return queryMatcher{t} }
