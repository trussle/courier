package models

import (
	"github.com/trussle/courier/pkg/uuid"
)

// Record represents a message from the underlying storage.
type Record interface {

	// ID of the record, which is unique to courier
	ID() uuid.UUID

	// Body is the payload of the record
	Body() []byte

	// RecordID is the potential id from the underlying provider
	RecordID() string

	// Receipt is the underlying uniqueness associated with the message
	Receipt() Receipt

	// Equal another Record or not
	Equal(Record) bool

	// Commit a record to a transaction
	Commit(Transaction) error

	// Failed a record to a transaction
	Failed(Transaction) error
}
