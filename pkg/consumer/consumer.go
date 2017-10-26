package consumer

import (
	"sync"
	"time"

	"github.com/SimonRichardson/resilience/retrier"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/trussle/courier/pkg/audit"
	"github.com/trussle/courier/pkg/http"
	"github.com/trussle/courier/pkg/lru"
	"github.com/trussle/courier/pkg/metrics"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/uuid"
)

// Consumer reads segments from the queue, and replicates merged segments to
// the rest of the cluster. It's implemented as a state machine: gather
// segments, replicate, commit, and repeat. All failures invalidate the entire
// batch.
type Consumer struct {
	mutex              sync.Mutex
	client             *http.Client
	queue              queue.Queue
	log                audit.Log
	lru                *lru.LRU
	gatherErrors       int
	stop               chan chan struct{}
	consumedSegments   metrics.Counter
	consumedRecords    metrics.Counter
	replicatedSegments metrics.Counter
	replicatedRecords  metrics.Counter
	failedSegments     metrics.Counter
	failedRecords      metrics.Counter
	gatherWaitTime     time.Duration
	logger             log.Logger
}

// New creates a consumer.
func New(
	client *http.Client,
	queue queue.Queue,
	log audit.Log,
	consumedSegments, consumedRecords metrics.Counter,
	replicatedSegments, replicatedRecords metrics.Counter,
	failedSegments, failedRecords metrics.Counter,
	logger log.Logger,
) *Consumer {
	consumer := &Consumer{
		mutex:              sync.Mutex{},
		client:             client,
		queue:              queue,
		log:                log,
		gatherErrors:       0,
		stop:               make(chan chan struct{}),
		consumedSegments:   consumedSegments,
		consumedRecords:    consumedRecords,
		replicatedSegments: replicatedSegments,
		replicatedRecords:  replicatedRecords,
		failedSegments:     failedSegments,
		failedRecords:      failedRecords,
		logger:             logger,
	}

	consumer.lru = lru.NewLRU(10, consumer.onElementEviction)

	return consumer
}

// Run consumes segments from the queue, and replicates them to the endpoint.
// Run returns when Stop is invoked.
func (c *Consumer) Run() {
	step := time.NewTicker(10 * time.Millisecond)
	defer step.Stop()

	state := c.gather
	for {
		select {
		case <-step.C:
			state = state()

		case q := <-c.stop:
			c.lru.Purge()
			close(q)
			return
		}
	}
}

// Stop the consumer from consuming.
func (c *Consumer) Stop() {
	q := make(chan struct{})
	c.stop <- q
	<-q
}

// stateFn is a lazy chaining mechism, similar to a trampoline, but via
// calls through Run.:
type stateFn func() stateFn

func (c *Consumer) gather() stateFn {
	// A naïve way to break out of the gather loop in atypical conditions.
	if c.gatherErrors > 0 {
		if c.lru.Len() == 0 {
			// We didn't successfully consume any segments.
			// Nothing to do but reset and try again.
			c.gatherErrors = 0
			return c.gather
		}
		// We consumed some segment, at least.
		// Press forward to persistence.
		return c.replicate
	}

	// More typical exit clauses.
	if c.lru.Capacity() {
		return c.replicate
	}

	// Dequeue
	var record models.Record
	select {
	case record = <-c.queue.Dequeue():
		break
	}

	c.lru.Add(record.ID(), record)

	c.consumedSegments.Inc()
	c.consumedRecords.Add(float64(1))

	return c.gather
}

func (c *Consumer) replicate() stateFn {
	var (
		base  = log.With(c.logger, "state", "replicate")
		warn  = level.Warn(base)
		debug = level.Debug(base)
	)

	if c.lru.Len() == 0 {
		warn.Log("action", "len", "reason", "len is zero")
		return c.gather
	}

	dequeued, err := c.lru.Dequeue(func(key uuid.UUID, value models.Record) error {
		debug.Log("action", "sending", "key", key.String())
		return c.client.Send(value.Body())
	})

	// even if we err out, we should send them in a transaction
	go func() {
		if err := c.commit(dequeued); err != nil {
			warn.Log("action", "commit", "err", err)
		}
	}()

	if err != nil {
		warn.Log("action", "dequeue", "err", err)
		return c.failure
	}

	c.replicatedSegments.Inc()
	c.replicatedRecords.Add(float64(len(dequeued)))

	return c.gather
}

func (c *Consumer) failure() stateFn {
	var (
		base = log.With(c.logger, "state", "replicate")
		warn = level.Warn(base)
	)

	txn := NewTransaction()
	for _, v := range c.lru.Slice() {
		if err := txn.Push(v.Value.ID(), v.Value); err != nil {
			continue
		}
	}
	if _, err := c.queue.Failed(txn); err != nil {
		warn.Log("state", "failure", "err", err)
		goto PURGE
	}

	c.failedSegments.Inc()
	c.failedRecords.Add(float64(txn.Len()))

PURGE:
	c.lru.Purge()
	return c.gather
}

func (c *Consumer) onElementEviction(reason lru.EvictionReason, key uuid.UUID, value models.Record) {
	// We should fail the transaction
	switch reason {
	case lru.Dequeued:
		// do nothing
	default:
		level.Warn(c.logger).Log("state", "eviction", "id", key.String(), "record", value.RecordID())
	}
}

func (c *Consumer) commit(queue []lru.KeyValue) error {
	txn := NewTransaction()
	for _, v := range queue {
		if err := txn.Push(v.Value.ID(), v.Value); err != nil {
			continue
		}
	}

	// Try and append to the audit log, if it fails do nothing but continue.
	try := retrier.New(3, 10*time.Millisecond)
	if err := try.Run(func() error {
		return c.log.Append(txn)
	}); err != nil {
		// do nothing here, we tried!
		level.Error(c.logger).Log("state", "commit", "err", err)
	}

	if _, err := c.queue.Commit(txn); err != nil {
		return err
	}

	return txn.Flush()
}
