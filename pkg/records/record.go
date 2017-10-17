package records

import (
	"math/rand"
	"reflect"
	"time"

	"github.com/trussle/courier/pkg/uuid"
)

// Record represents a message from a SQS queue that contains a unique ID, along
// with a SQS receipt handler and the body of the message.
// - ID is a unique id for the whole record
// - MessageID is unique for a queue
// It's import to understand the difference between ID and MessageID. The
// former is unique even if the same message is pulled.
type Record struct {
	ID        uuid.UUID
	MessageID string
	Receipt   string
	Body      []byte
	Timestamp time.Time
}

// Equals checks the equality of records against each other
func (r Record) Equals(other Record) bool {
	return r.ID.Equals(other.ID) &&
		reflect.DeepEqual(r.Body, other.Body)
}

// Generate allows UUID to be used within quickcheck scenarios.
func (Record) Generate(r *rand.Rand, size int) reflect.Value {
	rec, err := generate(r)
	if err != nil {
		panic(err)
	}
	return reflect.ValueOf(rec)
}

// Records represents a series of records that can be serialized and
// de-serialized
type Records []Record

// Append adds another record to the records slice
func (r *Records) Append(rec Record) {
	(*r) = append(*r, rec)
}

// Len returns the number of records with in the slice
func (r *Records) Len() int {
	return len(*r)
}

func generate(rnd *rand.Rand) (rec Record, err error) {
	if rec.ID, err = uuid.New(rnd); err != nil {
		return
	}

	// MessageID generation
	{
		dst := make([]byte, rnd.Intn(10)+20)
		if _, err = rnd.Read(dst); err != nil {
			return
		}
		rec.MessageID = string(dst)
	}

	// Receipt generation
	{
		dst := make([]byte, rnd.Intn(10)+24)
		if _, err = rnd.Read(dst); err != nil {
			return
		}
		rec.Receipt = string(dst)
	}

	// Body generation
	{
		dst := make([]byte, rnd.Intn(10)+48)
		if _, err = rnd.Read(dst); err != nil {
			return
		}
		rec.Body = dst
	}

	// Timestamp generation
	rec.Timestamp = time.Now().Round(time.Millisecond)

	return
}
