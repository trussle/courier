package models

import "github.com/trussle/courier/pkg/uuid"

// Transaction represents a way of managing stateful actions (commit vs failure)
type Transaction interface {

	// Push a record id and receipt to a transaction
	Push(uuid.UUID, Receipt) error

	// Walk over the transactions
	Walk(func(uuid.UUID, Receipt) error) error

	// Len returns the number of items with in a transaction
	Len() int

	// Flush a transaction
	Flush() error
}
