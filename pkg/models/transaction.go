package models

import "github.com/trussle/uuid"

// Transaction represents a way of managing statefull actions (commit vs
// failure)
type Transaction interface {

	// Push a record id and receipt to a transaction
	Push(uuid.UUID, Record) error

	// Walk over the transactions
	Walk(func(uuid.UUID, Record) error) error

	// Len returns the number of items with in a transaction
	Len() int

	// Flush a transaction
	Flush() error
}
