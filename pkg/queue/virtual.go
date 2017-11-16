package queue

import (
	"math/rand"

	"github.com/trussle/courier/pkg/models"
)

type virtualQueue struct {
	records []models.Record
}

func newVirtualQueue() Queue {
	return &virtualQueue{}
}

func (v *virtualQueue) Enqueue(rec models.Record) error {
	v.records = append(v.records, rec)
	return nil
}

func (v *virtualQueue) Dequeue() (res []models.Record, err error) {
	num := len(v.records)
	if num == 0 {
		return make([]models.Record, 0), nil
	}

	offset := max(1, rand.Intn(num))
	res, v.records = v.records[0:offset], v.records[offset:]
	return
}

func (v *virtualQueue) Commit(txn models.Transaction) (Result, error) {
	return Result{txn.Len(), 0}, nil
}

func (v *virtualQueue) Failed(txn models.Transaction) (Result, error) {
	return Result{txn.Len(), 0}, nil
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
