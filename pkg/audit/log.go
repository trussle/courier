package audit

import "github.com/trussle/courier/pkg/models"

// Log represents an audit log of transactions that have occurred.
type Log interface {

	// Append a transaction to the log
	Append(models.Transaction) error
}
