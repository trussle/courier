package queue

import (
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

type idRecord struct {
	ID     uuid.UUID
	Record models.Record
}

type transaction struct {
	values []idRecord
}

// NewTransaction creates a very simplistic models transaction
func NewTransaction() models.Transaction {
	return &transaction{}
}

func (t *transaction) Push(id uuid.UUID, record models.Record) error {
	t.values = append(t.values, idRecord{
		ID:     id,
		Record: record,
	})
	return nil
}

func (t *transaction) Walk(fn func(uuid.UUID, models.Record) error) error {
	for _, v := range t.values {
		if err := fn(v.ID, v.Record); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) Len() int { return len(t.values) }

func (t *transaction) Flush() error {
	t.values = t.values[:0]
	return nil
}
