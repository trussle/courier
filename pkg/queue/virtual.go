package queue

import (
	"math/rand"
	"sync"
	"time"

	"github.com/trussle/courier/pkg/uuid"
	"github.com/pkg/errors"
)

type virtualQueue struct {
	mutex      sync.Mutex
	stack      []*virtualSegment
	randSource *rand.Rand
}

// NewVirtualQueue yields an in-memory queue.
func NewVirtualQueue() Queue {
	return &virtualQueue{
		sync.Mutex{},
		make([]*virtualSegment, 0),
		rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (q *virtualQueue) Enqueue(r Record) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	id, err := uuid.New(q.randSource)
	if err != nil {
		return err
	}

	q.stack = append(q.stack, newVirtualSegment(id, []Record{r}))
	return nil
}

func (q *virtualQueue) Dequeue() (Segment, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.stack) == 0 {
		return nil, errNoSegmentsAvailable{errors.New("nothing found for reading")}
	}

	var s Segment
	s, q.stack = q.stack[0], q.stack[1:]

	return s, nil
}

func (q *virtualQueue) Reset() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.stack = make([]*virtualSegment, 0)

	return nil
}

type virtualSegment struct {
	id      uuid.UUID
	records []Record
}

func newVirtualSegment(id uuid.UUID, records []Record) *virtualSegment {
	return &virtualSegment{
		id:      id,
		records: records,
	}
}

func (v *virtualSegment) ID() uuid.UUID {
	return v.id
}

func (v *virtualSegment) Walk(fn func(Record) error) (err error) {
	for _, rec := range v.records {
		if err = fn(rec); err != nil {
			break
		}
	}
	return
}

func (v *virtualSegment) Commit(ids []uuid.UUID) (Result, error) {
	if len(v.records) == 0 {
		return Result{0, 0}, nil
	}

	result, _ := transactIDs(v.records, ids)
	v.records = v.records[:0]
	return result, nil
}

func (v *virtualSegment) Failed(ids []uuid.UUID) (Result, error) {
	if len(v.records) == 0 {
		return Result{0, 0}, nil
	}

	result, _ := transactIDs(v.records, ids)
	v.records = v.records[:0]
	return result, nil
}

func (v *virtualSegment) Size() int {
	return len(v.records)
}

func transactIDs(records []Record, ids []uuid.UUID) (Result, []Record) {
	// Fast exit
	if len(ids) == 0 {
		return Result{0, len(records)}, make([]Record, 0)
	}

	// Match items that are not in the ids
	var replicate []Record
	for _, v := range records {
		if contains(ids, v.ID) {
			replicate = append(replicate, v)
		}
	}

	replicated := len(replicate)
	return Result{replicated, len(records) - replicated}, replicate
}

func contains(ids []uuid.UUID, id uuid.UUID) bool {
	for _, v := range ids {
		if v.Equals(id) {
			return true
		}
	}
	return false
}
