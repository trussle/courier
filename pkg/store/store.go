package store

import (
	"github.com/trussle/courier/pkg/uuid"
)

// Store holds identifiers with associated records
type Store interface {

	// Add a transaction of identifiers to a associated to the store.
	Add([]uuid.UUID) error

	// Intersection reports back the union and difference of the identifiers
	// found with in the store.
	Intersection([]uuid.UUID) (union, difference []uuid.UUID, err error)
}
