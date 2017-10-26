package queue

import (
	"testing"
	"testing/quick"

	"github.com/golang/mock/gomock"
	"github.com/trussle/courier/pkg/models/mocks"
)

func TestVirtualQueue(t *testing.T) {
	t.Parallel()

	t.Run("enqueue", func(t *testing.T) {
		fn := func(r queueRecord) bool {
			queue := newVirtualQueue()
			go queue.Run()
			defer queue.Stop()

			err := queue.Enqueue(r)
			return err == nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue", func(t *testing.T) {
		fn := func(r queueRecord) bool {
			queue := newVirtualQueue()
			go queue.Run()
			defer queue.Stop()

			if err := queue.Enqueue(r); err != nil {
				t.Fatal(err)
			}

			rec := <-queue.Dequeue()
			return rec.Equal(r)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("commit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		queue := newVirtualQueue()
		go queue.Run()
		defer queue.Stop()

		txn := mocks.NewMockTransaction(ctrl)

		txn.EXPECT().Len().Return(0)

		res, err := queue.Commit(txn)
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 0, res.Success; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 0, res.Failure; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		queue := newVirtualQueue()
		go queue.Run()
		defer queue.Stop()

		txn := mocks.NewMockTransaction(ctrl)

		txn.EXPECT().Len().Return(0)

		res, err := queue.Failed(txn)
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 0, res.Success; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 0, res.Failure; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})
}
