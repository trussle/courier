package store

import "github.com/trussle/courier/pkg/uuid"

type nopStore struct{}

func newNopStore() Store {
	return nopStore{}
}

func (nopStore) Add([]uuid.UUID) error { return nil }
func (nopStore) Intersection(m []uuid.UUID) (union, difference []uuid.UUID, err error) {
	difference = m
	return
}
