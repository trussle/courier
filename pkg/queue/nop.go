package queue

import "github.com/trussle/courier/pkg/models"

type nopQueue struct{}

func newNopQueue() Queue {
	return &nopQueue{}
}

func (nopQueue) Enqueue(Record) error { return nil }
func (q nopQueue) Dequeue() <-chan Record {
	ch := make(chan Record, 1)
	ch <- nil
	return ch
}

func (nopQueue) Run()  {}
func (nopQueue) Stop() {}

func (nopQueue) Commit(txn models.Transaction) (Result, error) {
	return Result{txn.Len(), 0}, nil
}
func (nopQueue) Failed(txn models.Transaction) (Result, error) {
	return Result{txn.Len(), 0}, nil
}
