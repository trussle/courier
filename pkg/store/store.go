package store

// Store holds identifiers with associated records
type Store interface {

	// Add a transaction of identifiers to a associated to the store.
	Add([]string) error

	// Intersection reports back the union and difference of the identifiers
	// found with in the store.
	Intersection([]string) (union, difference []string, err error)
}
