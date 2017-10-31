package fifo

import (
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

// EvictionReason describes why the eviction happened
type EvictionReason int

const (
	// Purged by calling reset
	Purged EvictionReason = iota

	// Popped manually from the cache
	Popped

	// Removed manually from the cache
	Removed

	// Dequeued by walking over due to being dequeued
	Dequeued
)

type KeyValue struct {
	Key   uuid.UUID
	Value models.Record
}

// EvictCallback lets you know when an eviction has happened in the cache
type EvictCallback func(EvictionReason, uuid.UUID, models.Record)

type FIFO struct {
	items   []KeyValue
	onEvict EvictCallback
}

// NewFIFO implements a non-thread safe FIFO cache
func NewFIFO(onEvict EvictCallback) *FIFO {
	return &FIFO{
		items:   make([]KeyValue, 0),
		onEvict: onEvict,
	}
}

// Add adds a key, value pair.
func (f *FIFO) Add(key uuid.UUID, value models.Record) bool {
	f.items = append(f.items, KeyValue{
		Key:   key,
		Value: value,
	})
	return true
}

// Get returns back a value if it exists.
// Returns true if found.
func (f *FIFO) Get(key uuid.UUID) (models.Record, bool) {
	for _, v := range f.items {
		if v.Key.Equal(key) {
			return v.Value, true
		}
	}
	return nil, false
}

// Remove a value using it's key
// Returns true if a removal happened
func (f *FIFO) Remove(key uuid.UUID) bool {
	for k, v := range f.items {
		if v.Key.Equal(key) {
			f.items = append(f.items[:k], f.items[k+1:]...)
			f.onEvict(Removed, v.Key, v.Value)
			return true
		}
	}
	return false
}

// Contains finds out if a key is present in the LRU cache
func (f *FIFO) Contains(key uuid.UUID) bool {
	for _, v := range f.items {
		if v.Key.Equal(key) {
			return true
		}
	}
	return false
}

// Pop removes the last FIFO item with in the cache
func (f *FIFO) Pop() (uuid.UUID, models.Record, bool) {
	if len(f.items) == 0 {
		return uuid.Empty, nil, false
	}

	var kv KeyValue
	kv, f.items = f.items[0], f.items[1:]
	f.onEvict(Popped, kv.Key, kv.Value)
	return kv.Key, kv.Value, true
}

// Purge removes all items with in the cache, calling evict callback on each.
func (f *FIFO) Purge() {
	for _, v := range f.items {
		f.onEvict(Purged, v.Key, v.Value)
	}
	f.items = f.items[:0]
}

// Keys returns the keys as a slice
func (f *FIFO) Keys() []uuid.UUID {
	res := make([]uuid.UUID, len(f.items))
	for k, v := range f.items {
		res[k] = v.Key
	}
	return res
}

// Len returns the current length of the LRU cache
func (f *FIFO) Len() int {
	return len(f.items)
}

// Slice returns a snapshot of the KeyValue pairs.
func (f *FIFO) Slice() []KeyValue {
	return f.items[0:]
}

// Dequeue iterates over the LRU cache removing an item upon each iteration.
func (f *FIFO) Dequeue(fn func(uuid.UUID, models.Record) error) ([]KeyValue, error) {
	var dequeued []KeyValue
	for k, v := range f.items {
		if err := fn(v.Key, v.Value); err != nil {
			f.items = f.items[k:]
			return dequeued, err
		}
		f.onEvict(Dequeued, v.Key, v.Value)
		dequeued = append(dequeued, v)
	}

	f.items = f.items[:0]
	return dequeued, nil
}
