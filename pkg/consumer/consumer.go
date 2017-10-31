package consumer

import (
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/trussle/courier/pkg/audit"
	"github.com/trussle/courier/pkg/consumer/fifo"
	"github.com/trussle/courier/pkg/http"
	"github.com/trussle/courier/pkg/metrics"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/store"
	"github.com/trussle/courier/pkg/uuid"
)

const (
	defaultActiveTargetSize = 10
	defaultActiveTargetAge  = time.Minute
	defaultWaitTime         = time.Millisecond * 100
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
	store              store.Store
	fifo               *fifo.FIFO
	activeSince        time.Time
	activeTargetAge    time.Duration
	activeTargetSize   int
	gatherErrors       int
	waitTime           time.Duration
	replicationFactor  int
	stop               chan chan struct{}
	consumedSegments   metrics.Counter
	consumedRecords    metrics.Counter
	replicatedSegments metrics.Counter
	replicatedRecords  metrics.Counter
	failedSegments     metrics.Counter
	failedRecords      metrics.Counter
	logger             log.Logger
}

// New creates a consumer.
func New(
	client *http.Client,
	queue queue.Queue,
	log audit.Log,
	store store.Store,
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
		store:              store,
		activeSince:        time.Time{},
		activeTargetAge:    defaultActiveTargetAge,
		activeTargetSize:   defaultActiveTargetSize,
		gatherErrors:       0,
		waitTime:           defaultWaitTime,
		stop:               make(chan chan struct{}),
		consumedSegments:   consumedSegments,
		consumedRecords:    consumedRecords,
		replicatedSegments: replicatedSegments,
		replicatedRecords:  replicatedRecords,
		failedSegments:     failedSegments,
		failedRecords:      failedRecords,
		logger:             logger,
	}

	consumer.fifo = fifo.NewFIFO(consumer.onElementEviction)

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
			c.fifo.Purge()
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
		if c.fifo.Len() == 0 {
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
	var (
		tooBig = c.fifo.Len() > c.activeTargetSize
		tooOld = !c.activeSince.IsZero() && time.Since(c.activeSince) > c.activeTargetAge
	)
	if tooBig || tooOld {
		return c.replicate
	}

	// Dequeue
	records, err := c.queue.Dequeue()
	if err != nil {
		c.gatherErrors++
		return c.gather
	}

	if len(records) == 0 {
		time.Sleep(c.waitTime)
		return c.gather
	}

	// Find if any records have intersected with the store records.
	_, difference, err := c.store.Intersection(records)
	if err != nil {
		difference = records
	}

	for _, record := range difference {
		c.fifo.Add(record.ID(), record)
	}

	c.activeSince = time.Now()

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

	if c.fifo.Len() == 0 {
		warn.Log("action", "len", "reason", "len is zero")
		return c.gather
	}

	// We want to replicate all things first
	dequeued, err := c.fifo.Dequeue(func(key uuid.UUID, value models.Record) error {
		debug.Log("action", "sending", "key", key.String())
		return c.client.Send(value.Body())
	})

	// even if we err out, we should send them in a transaction
	if err := c.commit(dequeued); err != nil {
		warn.Log("action", "commit", "err", err)
	}

	if err != nil {
		warn.Log("action", "dequeue", "err", err)
		return c.failure
	}

	c.replicatedSegments.Inc()
	c.replicatedRecords.Add(float64(len(dequeued)))

	return c.gather
}

func (c *Consumer) failure() stateFn {
	txn := queue.NewTransaction()
	for _, v := range c.fifo.Slice() {
		if err := txn.Push(v.Value.ID(), v.Value); err != nil {
			continue
		}
	}
	if _, err := c.queue.Failed(txn); err != nil {
		level.Warn(c.logger).Log("state", "failure", "err", err)
		goto PURGE
	}

	c.failedSegments.Inc()
	c.failedRecords.Add(float64(txn.Len()))

PURGE:
	c.fifo.Purge()
	return c.gather
}

func (c *Consumer) onElementEviction(reason fifo.EvictionReason, key uuid.UUID, value models.Record) {
	// We should fail the transaction
	switch reason {
	case fifo.Dequeued:
		// do nothing
	default:
		level.Warn(c.logger).Log("state", "eviction", "id", key.String(), "record", value.RecordID())
	}
}

func (c *Consumer) commit(values []fifo.KeyValue) error {
	var (
		base = log.With(c.logger, "state", "commit")
		warn = level.Warn(base)
	)

	txn := queue.NewTransaction()
	for _, v := range values {
		if err := txn.Push(v.Value.ID(), v.Value); err != nil {
			continue
		}
	}

	// Try and append to the audit log, if it fails do nothing but continue.
	if err := c.log.Append(txn); err != nil {
		// do nothing here, we tried!
		warn.Log("state", "commit", "err", err)
	}

	if _, err := c.queue.Commit(txn); err != nil {
		return err
	}

	if _, err := c.store.Add(txn); err != nil {
		return err
	}

	return txn.Flush()
}
