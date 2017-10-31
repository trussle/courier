package store

type nopStore struct{}

func newNopStore() Store {
	return nopStore{}
}

func (nopStore) Add([]string) error { return nil }
func (nopStore) Intersection(m []string) (union, difference []string, err error) {
	difference = m
	return
}
