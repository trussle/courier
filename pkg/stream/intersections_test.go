package stream

import (
	"math/rand"
	"testing"
	"time"

	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/uuid"
)

func TestIntersection(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Run("empty segments with all query", func(t *testing.T) {
		segments := make([]queue.Segment, 0)
		union, difference := intersection(segments, All())

		if expected, actual := 0, len(union); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 0, len(difference); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("segments with all query", func(t *testing.T) {
		segments := []queue.Segment{
			newTestSegment(uuid.MustNew(rnd), []queue.Record{
				queue.Record{
					ID: uuid.MustNew(rnd),
				},
				queue.Record{
					ID: uuid.MustNew(rnd),
				},
			}),
			newTestSegment(uuid.MustNew(rnd), []queue.Record{
				queue.Record{
					ID: uuid.MustNew(rnd),
				},
			}),
		}
		union, difference := intersection(segments, All())

		if expected, actual := 2, len(union); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 0, len(difference); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("segments with empty selection", func(t *testing.T) {
		segments := []queue.Segment{
			newTestSegment(uuid.MustNew(rnd), []queue.Record{
				queue.Record{
					ID: uuid.MustNew(rnd),
				},
				queue.Record{
					ID: uuid.MustNew(rnd),
				},
			}),
			newTestSegment(uuid.MustNew(rnd), []queue.Record{
				queue.Record{
					ID: uuid.MustNew(rnd),
				},
			}),
		}
		union, difference := intersection(segments, NewQuery())

		if expected, actual := 0, len(union); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 2, len(difference); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("segments with matching selection", func(t *testing.T) {
		sid := uuid.MustNew(rnd)
		rid := uuid.MustNew(rnd)

		segments := []queue.Segment{
			newTestSegment(sid, []queue.Record{
				queue.Record{
					ID: rid,
				},
				queue.Record{
					ID: uuid.MustNew(rnd),
				},
			}),
			newTestSegment(uuid.MustNew(rnd), []queue.Record{
				queue.Record{
					ID: uuid.MustNew(rnd),
				},
			}),
		}
		query := NewQuery()
		query.Set(sid, []uuid.UUID{
			rid,
		})
		union, difference := intersection(segments, query)

		if expected, actual := 1, len(union); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 2, len(difference); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})
}

type testSegment struct {
	id      uuid.UUID
	records []queue.Record
}

func newTestSegment(id uuid.UUID, records []queue.Record) *testSegment {
	return &testSegment{
		id:      id,
		records: records,
	}
}

func (v *testSegment) ID() uuid.UUID {
	return v.id
}

func (v *testSegment) Walk(fn func(queue.Record) error) (err error) {
	for _, rec := range v.records {
		if err = fn(rec); err != nil {
			break
		}
	}
	return
}

func (v *testSegment) Commit(ids []uuid.UUID) (queue.Result, error) {
	if len(v.records) == 0 {
		return queue.Result{0, 0}, nil
	}

	result, _ := transactIDs(v.records, ids)
	v.records = v.records[:0]
	return result, nil
}

func (v *testSegment) Failed(ids []uuid.UUID) (queue.Result, error) {
	if len(v.records) == 0 {
		return queue.Result{0, 0}, nil
	}

	result, _ := transactIDs(v.records, ids)
	v.records = v.records[:0]
	return result, nil
}

func (v *testSegment) Size() int {
	return len(v.records)
}

func transactIDs(records []queue.Record, ids []uuid.UUID) (queue.Result, []queue.Record) {
	// Fast exit
	if len(ids) == 0 {
		return queue.Result{0, len(records)}, make([]queue.Record, 0)
	}

	// Match items that are not in the ids
	var replicate []queue.Record
	for _, v := range records {
		if contains(ids, v.ID) {
			replicate = append(replicate, v)
		}
	}

	replicated := len(replicate)
	return queue.Result{replicated, len(records) - replicated}, replicate
}
