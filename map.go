// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner

import (
	"fmt"
	"strings"
)

type entry struct {
	key   Value
	value interface{}
	next  *entry
}

// Map implements an associative array between Diviner values and Go
// values. A map is not itself a Value.
type Map struct {
	entries []*entry
	n       int
}

// NewMap returns a newly allocated Map.
func NewMap() *Map {
	return &Map{entries: make([]*entry, 32)}
}

func (m *Map) String() string {
	entries := make([]string, 0, m.Len())
	m.Range(func(key Value, value interface{}) {
		entries = append(entries, fmt.Sprintf("%s: %v", key, value))
	})
	return fmt.Sprintf("map[%s]", strings.Join(entries, ", "))
}

// Get retrieves the value associated by the key given by the
// provided value.
func (m *Map) Get(key Value) (val interface{}, ok bool) {
	bucket := int(Hash(key) % uint64(len(m.entries)))
	for entry := m.entries[bucket]; entry != nil; entry = entry.next {
		if entry.key.Equal(key) {
			return entry.value, true
		}
	}
	return nil, false
}

// Range iterates over all elements in the map.
func (m *Map) Range(fn func(key Value, value interface{})) {
	for _, entry := range m.entries {
		for ; entry != nil; entry = entry.next {
			fn(entry.key, entry.value)
		}
	}
}

// Put associated the value value with the provided key.
// Existing entries for a value is overridden.
func (m *Map) Put(key Value, value interface{}) {
	var (
		bucket = int(Hash(key) % uint64(len(m.entries)))
		p      = &m.entries[bucket]
		n      int
	)
	for *p != nil && !(*p).key.Equal(key) {
		p = &(*p).next
		n++
	}
	var next *entry
	if *p != nil {
		next = (*p).next
	} else {
		m.n++
	}
	*p = &entry{key: key, value: value, next: next}
	if n > 8 {
		m.rehash()
	}
}

// Len returns the number of entries in the map.
func (m *Map) Len() int {
	return m.n
}

func (m *Map) rehash() {
	var entries []*entry
	for _, entry := range m.entries {
		for entry != nil {
			entries = append(entries, entry)
			entry = entry.next
		}
	}
	m.entries = make([]*entry, len(entries)*2)
	for _, entry := range entries {
		bucket := int(Hash(entry.key) % uint64(len(m.entries)))
		entry.next = m.entries[bucket]
		m.entries[bucket] = entry
	}
}
