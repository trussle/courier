package queue

import "github.com/trussle/courier/pkg/uuid"

type nopQueue struct{}

// NewNopQueue has methods that always succeed, but do nothing.
func NewNopQueue() Queue {
	return nopQueue{}
}

func (q nopQueue) Enqueue(Record) error      { return nil }
func (q nopQueue) Dequeue() (Segment, error) { return nopSegment{}, nil }
func (q nopQueue) Reset() error              { return nil }

type nopSegment struct{}

func (v nopSegment) ID() uuid.UUID                      { return uuid.Empty }
func (v nopSegment) Commit([]uuid.UUID) (Result, error) { return Result{0, 0}, nil }
func (v nopSegment) Failed([]uuid.UUID) (Result, error) { return Result{0, 0}, nil }
func (v nopSegment) Size() int                          { return 0 }
func (v nopSegment) Walk(func(Record) error) error      { return nil }
