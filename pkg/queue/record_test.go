package queue

import (
	"math/rand"
	"reflect"
	"time"

	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

type TestRecord struct {
	id        uuid.UUID
	messageID string
	receipt   models.Receipt
	body      []byte
	timestamp time.Time
}

func (t TestRecord) ID() uuid.UUID {
	return t.id
}

func (t TestRecord) Body() []byte {
	return t.body
}

func (t TestRecord) Receipt() models.Receipt {
	return t.receipt
}

func (t TestRecord) Commit(txn models.Transaction) error {
	return txn.Push(t.id, t.receipt)
}

func (t TestRecord) Failed(txn models.Transaction) error {
	return txn.Push(t.id, t.receipt)
}

// Equal checks the equality of records against each other
func (t TestRecord) Equal(other Record) bool {
	return t.ID().Equal(other.ID()) &&
		reflect.DeepEqual(t.Body(), other.Body())
}

// Generate allows UUID to be used within quickcheck scenarios.
func (TestRecord) Generate(r *rand.Rand, size int) reflect.Value {
	rec, err := generate(r)
	if err != nil {
		panic(err)
	}
	return reflect.ValueOf(rec)
}

func generate(rnd *rand.Rand) (rec TestRecord, err error) {
	if rec.id, err = uuid.New(rnd); err != nil {
		return
	}

	// MessageID generation
	{
		dst := make([]byte, rnd.Intn(10)+20)
		if _, err = rnd.Read(dst); err != nil {
			return
		}
		rec.messageID = string(dst)
	}

	// Receipt generation
	{
		dst := make([]byte, rnd.Intn(10)+24)
		if _, err = rnd.Read(dst); err != nil {
			return
		}
		rec.receipt = models.Receipt(string(dst))
	}

	// Body generation
	{
		dst := make([]byte, rnd.Intn(10)+48)
		if _, err = rnd.Read(dst); err != nil {
			return
		}
		rec.body = dst
	}

	// Timestamp generation
	rec.timestamp = time.Now().Round(time.Millisecond)

	return
}
