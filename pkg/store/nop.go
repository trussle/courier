package store

import (
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

type nopStore struct{}

func newNopStore() Store {
	return nopStore{}
}

func (nopStore) Add(key uuid.UUID, value models.Record) error { return nil }
func (nopStore) Contains(key uuid.UUID) bool                  { return true }
