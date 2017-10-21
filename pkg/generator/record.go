package generator

import "github.com/trussle/courier/pkg/uuid"

type Receipt string

func (r Receipt) String() string {
	return string(r)
}

type Record interface {
	ID() uuid.UUID
	Body() []byte
	Receipt() Receipt
	Commit(Transaction) error
	Failed(Transaction) error
}

type Transaction interface {
	Push(uuid.UUID, Receipt) error
	Walk(func(uuid.UUID, Receipt) error) error
	Len() int
	Flush() error
}

type Result struct {
	Success, Failure int
}
