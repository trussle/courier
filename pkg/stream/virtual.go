package stream

import (
	"time"

	"github.com/trussle/courier/pkg/queue"
)

// Stream represents a series of active records
type virtualStream struct {
	active      []queue.Segment
	activeSince time.Time
	targetSize  int
	targetAge   time.Duration
}

// NewVirtualStream creates a new Stream with a size and age to know when a
// Stream is at a certain capacity
func newVirtualStream(size int, age time.Duration) *virtualStream {
	return &virtualStream{
		active:      make([]queue.Segment, 0),
		activeSince: time.Time{},
		targetSize:  size,
		targetAge:   age,
	}
}

// Len returns the number of available active records with in the Stream
func (l *virtualStream) Len() int {
	return len(l.active)
}

// Reset empties the virtualStream and puts it to a valid known state
func (l *virtualStream) Reset() error {
	l.active = l.active[:0]
	l.activeSince = time.Time{}

	return nil
}

// Capacity defines if the virtualStream is at a capacity. This is defined as if the
// virtualStream is over the target or age.
func (l *virtualStream) Capacity() bool {
	return l.Len() >= l.targetSize ||
		!l.activeSince.IsZero() && time.Since(l.activeSince) >= l.targetAge
}

// Append adds a segment with records to the virtualStream
func (l *virtualStream) Append(segment queue.Segment) error {
	l.active = append(l.active, segment)
	if l.activeSince.IsZero() {
		l.activeSince = time.Now()
	}
	return nil
}

// Walk allows the walking over each record sequentially
func (l *virtualStream) Walk(fn func(queue.Segment) error) error {
	for _, segment := range l.active {
		if err := fn(segment); err != nil {
			return err
		}
	}
	return nil
}

// Commit commits all the segments so that we can delete messages from the queue
func (l *virtualStream) Commit(input *Query) error {
	return l.resetVia(input, Flushed)
}

// Failed fails all the segments to make sure that we no longer work on those
// messages
func (l *virtualStream) Failed(input *Query) error {
	return l.resetVia(input, Failed)
}

func (l *virtualStream) resetVia(input *Query, reason Extension) error {
	union, difference := intersection(l.active, input)

	for segment, ids := range union {
		switch reason {
		case Failed:
			if _, err := segment.Failed(ids); err != nil {
				return err
			}
		case Flushed:
			if _, err := segment.Commit(ids); err != nil {
				return err
			}
		}
	}

	var segments []queue.Segment
	for segment := range difference {
		// Prevent empty segments from being reattached.
		if segment.Size() > 0 {
			segments = append(segments, segment)
		}
	}

	l.active = segments
	l.activeSince = time.Time{}

	return nil
}
