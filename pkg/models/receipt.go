package models

// Receipt represents a unique receipt for a record, so that it can be tracked
// for committing purposes.
type Receipt string

func (r Receipt) String() string {
	return string(r)
}
