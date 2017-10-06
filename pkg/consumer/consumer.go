package consumer

import (
	"sync"
	"time"

	"github.com/trussle/courier/pkg/http"
	"github.com/trussle/courier/pkg/metrics"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/stream"
	"github.com/trussle/courier/pkg/uuid"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

const (
	defaultWaitTime = time.Second
)

// Consumer reads segments from the queue, and replicates merged segments to
// the rest of the cluster. It's implemented as a state machine: gather
// segments, replicate, commit, and repeat. All failures invalidate the entire
// batch.
type Consumer struct {
	mutex              sync.Mutex
	client             *http.Client
	queue              queue.Queue
	stream             stream.Stream
	gatherErrors       int
	stop               chan chan struct{}
	consumedSegments   metrics.Counter
	consumedRecords    metrics.Counter
	replicatedSegments metrics.Counter
	replicatedRecords  metrics.Counter
	gatherWaitTime     time.Duration
	logger             log.Logger
}

// New creates a consumer.
func New(
	client *http.Client,
	queue queue.Queue,
	stream stream.Stream,
	consumedSegments, consumedRecords metrics.Counter,
	replicatedSegments, replicatedRecords metrics.Counter,
	logger log.Logger,
) *Consumer {
	return &Consumer{
		mutex:              sync.Mutex{},
		client:             client,
		queue:              queue,
		stream:             stream,
		gatherErrors:       0,
		stop:               make(chan chan struct{}),
		consumedSegments:   consumedSegments,
		consumedRecords:    consumedRecords,
		replicatedSegments: replicatedSegments,
		replicatedRecords:  replicatedRecords,
		gatherWaitTime:     defaultWaitTime,
		logger:             logger,
	}
}

// Run consumes segments from the queue, and replicates them to the endpoint.
// Run returns when Stop is invoked.
func (c *Consumer) Run() {
	step := time.NewTicker(100 * time.Millisecond)
	defer step.Stop()

	state := c.gather
	for {
		select {
		case <-step.C:
			state = state()

		case q := <-c.stop:
			if err := c.stream.Failed(stream.All()); err != nil {
				level.Warn(c.logger).Log("state", "stopping", "err", err)
			}
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
	var (
		base = log.With(c.logger, "state", "gather")
		warn = level.Warn(base)
	)

	warn.Log("state", "gather")

	// A naïve way to break out of the gather loop in atypical conditions.
	if c.gatherErrors > 0 {
		if c.stream.Len() == 0 {
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
	if c.stream.Capacity() {
		return c.replicate
	}

	segment, err := c.queue.Dequeue()
	if err != nil {
		// Normal, when the ingester has no more segments to give right now.
		// after enough of these errors, we should replicate
		warn.Log("reason", "dequeuing", "err", err)
		c.gatherErrors++
		return c.gather
	}

	if err := c.stream.Append(segment); err != nil {
		warn.Log("reason", "appending records", "err", err)
		c.gatherErrors++
		return c.failure
	}

	c.consumedSegments.Inc()
	c.consumedRecords.Add(float64(segment.Size()))

	return c.gather
}

func (c *Consumer) replicate() stateFn {
	var (
		base = log.With(c.logger, "state", "replicate")
		warn = level.Warn(base)
	)

	replicated := stream.NewTransaction()

	// Replicate the records to the endpoint
	if err := c.stream.Walk(func(segment queue.Segment) error {
		var ids []uuid.UUID

		err := segment.Walk(func(record queue.Record) error {
			if err := c.client.Send(record.Body); err != nil {
				return err
			}

			// Append only when replicated
			ids = append(ids, record.ID)
			return nil
		})

		// Replicate any IDs that was successful
		replicated.Set(segment.ID(), ids[0:])
		return err
	}); err != nil {
		warn.Log("state", "replicate", "err", err)
		return c.failure
	}

	// All good!
	c.replicatedSegments.Inc()
	c.replicatedRecords.Add(float64(replicated.Len()))

	if err := c.stream.Commit(replicated); err != nil {
		warn.Log("err", err)
	}

	return c.gather
}

func (c *Consumer) failure() stateFn {
	var (
		base = log.With(c.logger, "state", "failure")
		warn = level.Warn(base)
	)

	if err := c.stream.Failed(stream.All()); err != nil {
		warn.Log("err", err)
	}

	return c.gather
}
