package queue

import (
	"math/rand"
	"reflect"
	"time"

	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

type queueRecord struct {
	id         uuid.UUID
	messageID  string
	receipt    models.Receipt
	body       []byte
	receivedAt time.Time
}

// NewRecord is a default queue record implementation
func NewRecord(id uuid.UUID,
	messageID string,
	receipt models.Receipt,
	body []byte,
	receivedAt time.Time,
) models.Record {
	return queueRecord{
		id:         id,
		messageID:  messageID,
		receipt:    receipt,
		body:       body,
		receivedAt: receivedAt,
	}
}

func (r queueRecord) ID() uuid.UUID           { return r.id }
func (r queueRecord) Receipt() models.Receipt { return r.receipt }
func (r queueRecord) RecordID() string        { return r.messageID }
func (r queueRecord) Body() []byte            { return r.body }

func (r queueRecord) Equal(other models.Record) bool {
	return r.ID().Equal(other.ID()) &&
		reflect.DeepEqual(r.Body(), other.Body())
}

func (r queueRecord) Commit(txn models.Transaction) error {
	return txn.Push(r.id, r)
}

func (r queueRecord) Failed(txn models.Transaction) error {
	return txn.Push(r.id, r)
}

// Generate allows UUID to be used within quickcheck scenarios.
func (queueRecord) Generate(r *rand.Rand, size int) reflect.Value {
	rec, err := GenerateQueueRecord(r)
	if err != nil {
		panic(err)
	}
	return reflect.ValueOf(rec)
}

// GenerateQueueRecord creates a new queue record
func GenerateQueueRecord(rnd *rand.Rand) (models.Record, error) {
	var (
		err error
		rec = queueRecord{}
	)

	if rec.id, err = uuid.New(rnd); err != nil {
		return nil, err
	}

	// MessageID generation
	{
		dst := make([]byte, rnd.Intn(10)+20)
		if _, err = rnd.Read(dst); err != nil {
			return nil, err
		}
		rec.messageID = string(dst)
	}

	// Receipt generation
	{
		dst := make([]byte, rnd.Intn(10)+24)
		if _, err = rnd.Read(dst); err != nil {
			return nil, err
		}
		rec.receipt = models.Receipt(string(dst))
	}

	// Body generation
	{
		dst := make([]byte, rnd.Intn(10)+48)
		if _, err = rnd.Read(dst); err != nil {
			return nil, err
		}
		rec.body = dst
	}

	// Timestamp generation
	rec.receivedAt = time.Now().Round(time.Millisecond)

	return rec, nil
}
