package consumer

import (
	"errors"
	"math/rand"
	nhttp "net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	auditMocks "github.com/trussle/courier/pkg/audit/mocks"
	"github.com/trussle/courier/pkg/consumer/fifo"
	"github.com/trussle/courier/pkg/http"
	metricsMocks "github.com/trussle/courier/pkg/metrics/mocks"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/queue"
	queueMocks "github.com/trussle/courier/pkg/queue/mocks"
	"github.com/trussle/harness/matchers"
	"github.com/trussle/uuid"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	t.Run("run", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			wg sync.WaitGroup

			client             = http.NewClient(nhttp.DefaultClient, "")
			queue              = queueMocks.NewMockQueue(ctrl)
			audit              = auditMocks.NewMockLog(ctrl)
			consumedSegments   = metricsMocks.NewMockCounter(ctrl)
			consumedRecords    = metricsMocks.NewMockCounter(ctrl)
			replicatedSegments = metricsMocks.NewMockCounter(ctrl)
			replicatedRecords  = metricsMocks.NewMockCounter(ctrl)
			failedSegments     = metricsMocks.NewMockCounter(ctrl)
			failedRecords      = metricsMocks.NewMockCounter(ctrl)
		)
		consumer := New(client,
			queue,
			audit,
			time.Second,
			consumedSegments,
			consumedRecords,
			replicatedSegments,
			replicatedRecords,
			failedSegments,
			failedRecords,
			log.NewNopLogger(),
		)

		wg.Add(1)

		go func() {
			wg.Done()
			consumer.Run()
		}()

		wg.Wait()

		consumer.Stop()
	})
}

func TestConsumerGather(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Run("gather with errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		consumer := &Consumer{}
		consumer.gatherErrors = 1
		consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)

		if expected, actual := consumer.gather, consumer.gather(); !funcEquality(expected, actual) {
			t.Errorf("expected: %T, actual: %T", expected, actual)
		}
	})

	t.Run("gather with errors but with values", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			consumer := &Consumer{}
			consumer.gatherErrors = 1
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			if expected, actual := consumer.replicate, consumer.gather(); !funcEquality(expected, actual) {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("gather that is too big", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			consumer := &Consumer{}
			consumer.activeTargetSize = 1
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)
			consumer.fifo.Add(id, record)

			if expected, actual := consumer.replicate, consumer.gather(); !funcEquality(expected, actual) {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("gather with dequeue error", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			queue := queueMocks.NewMockQueue(ctrl)
			queue.EXPECT().Dequeue().Return(nil, errors.New("bad"))

			consumer := &Consumer{}
			consumer.queue = queue
			consumer.activeTargetSize = 100
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			if expected, actual := consumer.gather, consumer.gather(); !funcEquality(expected, actual) {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("gather with dequeue no records", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			queue := queueMocks.NewMockQueue(ctrl)
			queue.EXPECT().Dequeue().Return([]models.Record{}, nil)

			consumer := &Consumer{}
			consumer.waitTime = time.Nanosecond
			consumer.queue = queue
			consumer.activeTargetSize = 100
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			if expected, actual := consumer.gather, consumer.gather(); !funcEquality(expected, actual) {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("gather with cache error", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			var (
				queue            = queueMocks.NewMockQueue(ctrl)
				audit            = auditMocks.NewMockLog(ctrl)
				consumedSegments = metricsMocks.NewMockCounter(ctrl)
				consumedRecords  = metricsMocks.NewMockCounter(ctrl)
			)

			queue.EXPECT().Dequeue().Return([]models.Record{
				record,
			}, nil)
			consumedSegments.EXPECT().Inc()
			consumedRecords.EXPECT().Add(matchers.MatchAnyFloat64())

			consumer := &Consumer{}
			consumer.waitTime = time.Nanosecond
			consumer.queue = queue
			consumer.log = audit
			consumer.activeTargetSize = 100
			consumer.consumedSegments = consumedSegments
			consumer.consumedRecords = consumedRecords
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			if expected, actual := consumer.gather, consumer.gather(); !funcEquality(expected, actual) {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("gather", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			var (
				queue            = queueMocks.NewMockQueue(ctrl)
				audit            = auditMocks.NewMockLog(ctrl)
				consumedSegments = metricsMocks.NewMockCounter(ctrl)
				consumedRecords  = metricsMocks.NewMockCounter(ctrl)
			)

			queue.EXPECT().Dequeue().Return([]models.Record{
				record,
			}, nil)
			consumedSegments.EXPECT().Inc()
			consumedRecords.EXPECT().Add(matchers.MatchAnyFloat64())

			consumer := &Consumer{}
			consumer.waitTime = time.Nanosecond
			consumer.queue = queue
			consumer.log = audit
			consumer.activeTargetSize = 100
			consumer.consumedSegments = consumedSegments
			consumer.consumedRecords = consumedRecords
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			if expected, actual := consumer.gather, consumer.gather(); !funcEquality(expected, actual) {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestConsumerReplicate(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Run("replicate with no records", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		consumer := &Consumer{}
		consumer.logger = log.NewNopLogger()
		consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)

		if expected, actual := consumer.gather, consumer.replicate(); !funcEquality(expected, actual) {
			t.Errorf("expected: %T, actual: %T", expected, actual)
		}
	})

	t.Run("replicate with dequeue error", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			var (
				queue = queueMocks.NewMockQueue(ctrl)
				audit = auditMocks.NewMockLog(ctrl)
			)

			audit.EXPECT().Append(gomock.Any())
			queue.EXPECT().Commit(gomock.Any())

			mux := nhttp.NewServeMux()
			mux.HandleFunc("/", func(w nhttp.ResponseWriter, r *nhttp.Request) {
				w.WriteHeader(nhttp.StatusNotFound)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			consumer := &Consumer{}
			consumer.log = audit
			consumer.queue = queue
			consumer.logger = log.NewNopLogger()
			consumer.client = http.NewClient(nhttp.DefaultClient, server.URL)
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			if expected, actual := consumer.failure, consumer.replicate(); !funcEquality(expected, actual) {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("replicate", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			var (
				queue              = queueMocks.NewMockQueue(ctrl)
				audit              = auditMocks.NewMockLog(ctrl)
				replicatedSegments = metricsMocks.NewMockCounter(ctrl)
				replicatedRecords  = metricsMocks.NewMockCounter(ctrl)
			)

			audit.EXPECT().Append(gomock.Any())
			queue.EXPECT().Commit(gomock.Any())
			replicatedSegments.EXPECT().Inc()
			replicatedRecords.EXPECT().Add(float64(1))

			mux := nhttp.NewServeMux()
			mux.HandleFunc("/", func(w nhttp.ResponseWriter, r *nhttp.Request) {
				w.WriteHeader(nhttp.StatusOK)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			consumer := &Consumer{}
			consumer.log = audit
			consumer.queue = queue
			consumer.logger = log.NewNopLogger()
			consumer.client = http.NewClient(nhttp.DefaultClient, server.URL)
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)
			consumer.replicatedSegments = replicatedSegments
			consumer.replicatedRecords = replicatedRecords

			if expected, actual := consumer.gather, consumer.replicate(); !funcEquality(expected, actual) {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestConsumerFailure(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Run("failure with queue error", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			que := queueMocks.NewMockQueue(ctrl)
			que.EXPECT().Failed(gomock.Any()).Return(queue.Result{}, errors.New("bad"))

			consumer := &Consumer{}
			consumer.queue = que
			consumer.logger = log.NewNopLogger()
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			if expected, actual := consumer.gather, consumer.failure(); !funcEquality(expected, actual) {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("failure", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			var (
				queue          = queueMocks.NewMockQueue(ctrl)
				failedSegments = metricsMocks.NewMockCounter(ctrl)
				failedRecords  = metricsMocks.NewMockCounter(ctrl)
			)

			queue.EXPECT().Failed(gomock.Any())
			failedSegments.EXPECT().Inc()
			failedRecords.EXPECT().Add(float64(1))

			consumer := &Consumer{}
			consumer.queue = queue
			consumer.logger = log.NewNopLogger()
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)
			consumer.failedSegments = failedSegments
			consumer.failedRecords = failedRecords

			if expected, actual := consumer.gather, consumer.failure(); !funcEquality(expected, actual) {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestConsumerCommit(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	t.Run("commit with log failure", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			var (
				queue = queueMocks.NewMockQueue(ctrl)
				audit = auditMocks.NewMockLog(ctrl)
			)

			audit.EXPECT().Append(gomock.Any()).Return(errors.New("bad"))
			queue.EXPECT().Commit(gomock.Any())

			consumer := &Consumer{}
			consumer.log = audit
			consumer.queue = queue
			consumer.logger = log.NewNopLogger()
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			err = consumer.commit(consumer.fifo.Slice())
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("commit with cache failure", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			var (
				queue = queueMocks.NewMockQueue(ctrl)
				audit = auditMocks.NewMockLog(ctrl)
			)

			audit.EXPECT().Append(gomock.Any())
			queue.EXPECT().Commit(gomock.Any())

			consumer := &Consumer{}
			consumer.log = audit
			consumer.queue = queue
			consumer.logger = log.NewNopLogger()
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			err = consumer.commit(consumer.fifo.Slice())
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("commit with queue failure", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			var (
				que   = queueMocks.NewMockQueue(ctrl)
				audit = auditMocks.NewMockLog(ctrl)
			)

			audit.EXPECT().Append(gomock.Any())
			que.EXPECT().Commit(gomock.Any()).Return(queue.Result{}, errors.New("bad"))

			consumer := &Consumer{}
			consumer.log = audit
			consumer.queue = que
			consumer.logger = log.NewNopLogger()
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			err = consumer.commit(consumer.fifo.Slice())
			if expected, actual := true, err != nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("commit", func(t *testing.T) {
		fn := func(id uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			record, err := queue.GenerateQueueRecord(rnd)
			if err != nil {
				t.Fatal(err)
			}

			var (
				queue = queueMocks.NewMockQueue(ctrl)
				audit = auditMocks.NewMockLog(ctrl)
			)

			audit.EXPECT().Append(gomock.Any())
			queue.EXPECT().Commit(gomock.Any())

			consumer := &Consumer{}
			consumer.log = audit
			consumer.queue = queue
			consumer.logger = log.NewNopLogger()
			consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)
			consumer.fifo.Add(id, record)

			err = consumer.commit(consumer.fifo.Slice())
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func funcEquality(a, b stateFn) bool {
	return runtime.FuncForPC(reflect.ValueOf(a).Pointer()).Name() ==
		runtime.FuncForPC(reflect.ValueOf(b).Pointer()).Name()
}
