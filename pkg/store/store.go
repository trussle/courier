package store

import (
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

// Store holds records with associated identifiers
type Store interface {

	// Add a record to a associated to the store.
	Add(uuid.UUID, models.Record) error

	// Contains checks to see a record is stored with in the store.
	Contains(uuid.UUID) bool
}
