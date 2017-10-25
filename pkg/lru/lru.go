package lru

import (
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

type KeyValue struct {
	Key   uuid.UUID
	Value models.Record
}

// EvictCallback lets you know when an eviction has happened in the cache
type EvictCallback func(uuid.UUID, models.Record)

// LRU implements a non-thread safe fixed size LRU cache
type LRU struct {
	size    int
	items   map[uuid.UUID]*element
	list    list
	onEvict EvictCallback
}

// NewLRU creates a LRU cache with a size and callback on eviction
func NewLRU(size int, onEvict EvictCallback) *LRU {
	return &LRU{
		size:    size,
		items:   make(map[uuid.UUID]*element),
		onEvict: onEvict,
	}
}

// Add adds a key, value pair.
// Returns true if an eviction happened.
func (l *LRU) Add(key uuid.UUID, value models.Record) bool {
	if elem, ok := l.items[key]; ok {
		l.list.Mark(elem)
		elem.value = value
		return false
	}

	elem := &element{
		key:   key,
		value: value,
	}
	l.list.Unshift(elem)
	l.items[key] = elem

	if l.list.Len() > l.size {
		l.Pop()
		return true
	}
	return false
}

// Get returns back a value if it exists.
// Returns true if found.
func (l *LRU) Get(key uuid.UUID) (value models.Record, ok bool) {
	var elem *element
	if elem, ok = l.items[key]; ok {
		l.list.Mark(elem)
		value = elem.value
	}
	return
}

// Remove a value using it's key
// Returns true if a removal happened
func (l *LRU) Remove(key uuid.UUID) (ok bool) {
	var elem *element
	if elem, ok = l.items[key]; ok {
		l.removeElement(elem)
	}
	return
}

// Peek returns a value, without marking the LRU cache.
// Returns true if a value is found.
func (l *LRU) Peek(key uuid.UUID) (value models.Record, ok bool) {
	var elem *element
	if elem, ok = l.items[key]; ok {
		value = elem.value
	}
	return
}

// Contains finds out if a key is present in the LRU cache
func (l *LRU) Contains(key uuid.UUID) bool {
	_, ok := l.items[key]
	return ok
}

// Pop removes the last LRU item with in the cache
func (l *LRU) Pop() (uuid.UUID, models.Record, bool) {
	if elem := l.list.Back(); elem != nil {
		l.removeElement(elem)
		return elem.key, elem.value, true
	}
	return uuid.Empty, nil, false
}

// Purge removes all items with in the cache, calling evict callback on each.
func (l *LRU) Purge() {
	l.list.Walk(func(key uuid.UUID, value models.Record) {
		if l.onEvict != nil {
			l.onEvict(key, value)
		}
		delete(l.items, key)
	})
	l.list.Reset()
}

// Keys returns the keys as a slice
func (l *LRU) Keys() []uuid.UUID {
	var (
		index int
		keys  = make([]uuid.UUID, l.list.Len())
	)
	l.list.Walk(func(k uuid.UUID, v models.Record) {
		keys[index] = k
		index++
	})
	return keys
}

// Len returns the current length of the LRU cache
func (l *LRU) Len() int {
	return l.list.Len()
}

// Capacity returns if the LRU cache is at capacity or not.
func (l *LRU) Capacity() bool {
	return l.size == l.Len()
}

// Slice returns a snapshot of the KeyValue pairs.
func (l *LRU) Slice() []KeyValue {
	var (
		index  int
		values = make([]KeyValue, l.list.Len())
	)
	l.list.Walk(func(k uuid.UUID, v models.Record) {
		values[index] = KeyValue{
			Key:   k,
			Value: v,
		}
		index++
	})
	return values
}

// Dequeue iterates over the LRU cache removing an item upon each iteration.
func (l *LRU) Dequeue(fn func(uuid.UUID, models.Record) error) ([]KeyValue, error) {
	var dequeued []*element
	err := l.list.Dequeue(func(e *element) error {
		err := fn(e.key, e.value)
		if err == nil {
			dequeued = append(dequeued, e)
		}
		return err
	})

	res := make([]KeyValue, len(dequeued))
	for k, e := range dequeued {
		l.removeElement(e)
		res[k] = KeyValue{
			Key:   e.key,
			Value: e.value,
		}
	}
	return res, err
}

func (l *LRU) removeElement(e *element) {
	ok := l.list.Remove(e)
	delete(l.items, e.key)
	if ok && l.onEvict != nil {
		l.onEvict(e.key, e.value)
	}
}
