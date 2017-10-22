package queue

import "github.com/trussle/courier/pkg/models"

type virtualQueue struct {
	stop    chan chan struct{}
	records chan Record
}

func newVirtualQueue() Queue {
	return &virtualQueue{
		stop:    make(chan chan struct{}),
		records: make(chan Record, 1),
	}
}

func (v *virtualQueue) Enqueue(rec Record) error {
	v.records <- rec
	return nil
}

func (v *virtualQueue) Dequeue() <-chan Record {
	return v.records
}

func (v *virtualQueue) Run() {
	for {
		select {
		case q := <-v.stop:
			close(q)
			return
		}
	}
}

func (v *virtualQueue) Stop() {
	q := make(chan struct{})
	v.stop <- q
	<-q
}

func (v *virtualQueue) Commit(txn models.Transaction) (Result, error) {
	return Result{txn.Len(), 0}, nil
}

func (v *virtualQueue) Failed(txn models.Transaction) (Result, error) {
	return Result{txn.Len(), 0}, nil
}
