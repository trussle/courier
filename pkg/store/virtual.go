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

func (v *virtualStore) Add(key uuid.UUID, value models.Record) error {
	v.lru.Add(key, value)
	return nil
}

func (v *virtualStore) Contains(key uuid.UUID) bool {
	return v.lru.Contains(key)
}

func (v *virtualStore) onElementEviction(reason lru.EvictionReason, key uuid.UUID, value models.Record) {
	// do nothing
}
