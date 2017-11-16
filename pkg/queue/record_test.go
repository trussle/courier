package queue

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/models/mocks"
	"github.com/trussle/harness/matchers"
	"github.com/trussle/uuid"
)

func TestRecord(t *testing.T) {
	t.Parallel()

	t.Run("new record", func(t *testing.T) {
		fn := func(id uuid.UUID, messageID, receipt string, body []byte) bool {
			now := time.Now()
			record := NewRecord(id, messageID, models.Receipt(receipt), body, now)

			return record.ID().Equals(id) &&
				record.Receipt().String() == receipt &&
				record.RecordID() == messageID &&
				reflect.DeepEqual(record.Body(), body)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("record equals", func(t *testing.T) {
		fn := func(rec queueRecord) bool {
			return rec.Equal(rec)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("record not equals", func(t *testing.T) {
		fn := func(rec0, rec1 queueRecord) bool {
			return !rec0.Equal(rec1)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("commit", func(t *testing.T) {
		fn := func(rec queueRecord) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			txn := mocks.NewMockTransaction(ctrl)

			txn.EXPECT().Push(matchers.MatchUUID(rec.ID()), CompareRecord(rec)).Return(nil)

			if err := rec.Commit(txn); err != nil {
				t.Fatal(err)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("failed", func(t *testing.T) {
		fn := func(rec queueRecord) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			txn := mocks.NewMockTransaction(ctrl)

			txn.EXPECT().Push(matchers.MatchUUID(rec.ID()), CompareRecord(rec)).Return(nil)

			if err := rec.Failed(txn); err != nil {
				t.Fatal(err)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

type recordMatcher struct {
	record models.Record
}

func (m recordMatcher) Matches(x interface{}) bool {
	d, ok := x.(models.Record)
	if !ok {
		return false
	}

	return m.record.Equal(d)
}

func (recordMatcher) String() string {
	return "is record"
}

func CompareRecord(doc models.Record) gomock.Matcher {
	return recordMatcher{doc}
}
