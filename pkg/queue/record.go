package queue

import (
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

// Record represents a message from the underlying storage.
type Record interface {

	// ID of the record
	ID() uuid.UUID

	// Body is the payload of the record
	Body() []byte

	// Receipt is the underlying uniqueness associated with the message
	Receipt() models.Receipt

	// Equal another Record or not
	Equal(Record) bool

	// Commit a record to a transaction
	Commit(models.Transaction) error

	// Failed a record to a transaction
	Failed(models.Transaction) error
}

// Result returns the amount of successes and failures
type Result struct {
	Success, Failure int
}
