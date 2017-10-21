package lru

type Key interface{}
type Value interface{}
type KeyValue struct {
	Key   Key
	Value Value
}

type EvictCallback func(Key, Value)

// LRU implements a non-thread safe fixed size LRU cache
type LRU struct {
	size    int
	items   map[Key]*element
	list    list
	onEvict EvictCallback
}

func NewLRU(size int, onEvict EvictCallback) *LRU {
	return &LRU{
		size:    size,
		items:   make(map[Key]*element),
		onEvict: onEvict,
	}
}

func (l *LRU) Add(key Key, value Value) bool {
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

func (l *LRU) Get(key Key) (value Value, ok bool) {
	var elem *element
	if elem, ok = l.items[key]; ok {
		l.list.Mark(elem)
		value = elem.value
	}
	return
}

func (l *LRU) Remove(key Key) (ok bool) {
	var elem *element
	if elem, ok = l.items[key]; ok {
		l.removeElement(elem)
	}
	return
}

func (l *LRU) Peek(key Key) (value Value, ok bool) {
	var elem *element
	if elem, ok = l.items[key]; ok {
		value = elem.value
	}
	return
}

func (l *LRU) Contains(key Key) bool {
	_, ok := l.items[key]
	return ok
}

func (l *LRU) Pop() (Key, Value, bool) {
	if elem := l.list.Back(); elem != nil {
		l.removeElement(elem)
		return elem.key, elem.value, true
	}
	return nil, nil, false
}

func (l *LRU) Purge() {
	l.list.Walk(func(key Key, value Value) {
		if l.onEvict != nil {
			l.onEvict(key, value)
		}
		delete(l.items, key)
	})
	l.list.Reset()
}

func (l *LRU) Keys() []Key {
	var (
		index int
		keys  = make([]Key, l.list.Len())
	)
	l.list.Walk(func(k Key, v Value) {
		keys[index] = k
		index++
	})
	return keys
}

func (l *LRU) Len() int {
	return l.list.Len()
}

func (l *LRU) Slice() []KeyValue {
	var (
		index  int
		values = make([]KeyValue, l.list.Len())
	)
	l.list.Walk(func(k Key, v Value) {
		values[index] = KeyValue{
			Key:   k,
			Value: v,
		}
		index++
	})
	return values
}

func (l *LRU) removeElement(e *element) {
	ok := l.list.Remove(e)
	delete(l.items, e.key)
	if ok && l.onEvict != nil {
		l.onEvict(e.key, e.value)
	}
}
