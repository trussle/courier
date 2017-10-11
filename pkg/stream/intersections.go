package stream

import (
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/uuid"
)

func intersection(segments []queue.Segment, input *Query) (map[queue.Segment][]uuid.UUID, map[queue.Segment][]uuid.UUID) {
	var (
		union      = make(map[queue.Segment][]uuid.UUID)
		difference = make(map[queue.Segment][]uuid.UUID)
	)

	for _, segment := range segments {

		// Everything is a union, there are no differences
		if input.All() {
			if err := segment.Walk(func(record queue.Record) error {
				union[segment] = append(union[segment], record.ID)
				return nil
			}); err != nil {
				continue
			}

			// We're done here, move on!
			continue
		}

		// Find union and differences from the input
		potential, ok := input.Get(segment.ID())
		if err := segment.Walk(func(record queue.Record) error {
			// Nothing found at all, so push everything to difference
			if !ok {
				difference[segment] = append(difference[segment], record.ID)
				return nil
			}

			// If something found and is found in potential haystack add it to the
			// union.
			if contains(potential, record.ID) {
				union[segment] = append(union[segment], record.ID)
			} else {
				difference[segment] = append(union[segment], record.ID)
			}
			return nil
		}); err != nil {
			continue
		}
	}
	return union, difference
}
