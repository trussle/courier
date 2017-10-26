package consumer

import (
	"github.com/trussle/courier/pkg/lru"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

type transaction struct {
	values []lru.KeyValue
}

// NewTransaction creates a very simplistic models transaction
func NewTransaction() models.Transaction {
	return &transaction{}
}

func (t *transaction) Push(id uuid.UUID, record models.Record) error {
	t.values = append(t.values, lru.KeyValue{
		Key:   id,
		Value: record,
	})
	return nil
}

func (t *transaction) Walk(fn func(uuid.UUID, models.Record) error) error {
	for _, v := range t.values {
		if err := fn(v.Key, v.Value); err != nil {
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
