package store

import (
	"github.com/trussle/courier/pkg/lru"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

type remoteStore struct {
	lru *lru.LRU
}

func newRemoteStore(size int) Store {
	store := &remoteStore{}
	store.lru = lru.NewLRU(size, store.onElementEviction)
	return store
}

func (v *remoteStore) Add(txn models.Transaction) error {

}

func (v *remoteStore) Intersection(m []models.Record) (union, difference []models.Record, err error) {

}

func (v *remoteStore) onElementEviction(reason lru.EvictionReason, key uuid.UUID, value models.Record) {
	// do nothing
}
