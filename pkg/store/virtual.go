package store

import (
	"github.com/trussle/courier/pkg/lru"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

type virtualStore struct {
	lru *lru.LRU
}

func newVirtualStore(size int) Store {
	store := &virtualStore{}
	store.lru = lru.NewLRU(size, store.onElementEviction)
	return store
}

func (v *virtualStore) Add(txn models.Transaction) error {
	return txn.Walk(func(key uuid.UUID, value models.Record) error {
		v.lru.Add(key, value)
		return nil
	})
}

func (v *virtualStore) Intersection(m []models.Record) (union, difference []models.Record, err error) {
	for _, r := range m {
		if v.lru.Contains(r.ID()) {
			union = append(union, r)
		} else {
			difference = append(difference, r)
		}
	}
	return
}

func (v *virtualStore) onElementEviction(reason lru.EvictionReason, key uuid.UUID, value models.Record) {
	// do nothing
}
