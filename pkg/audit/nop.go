package audit

import "github.com/trussle/courier/pkg/models"

type nop struct{}

func newNopLog() Log { return nop{} }

func (nop) Append(models.Transaction) error { return nil }
