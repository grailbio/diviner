// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner

import (
	"encoding/gob"
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/grailbio/base/writehash"
)

func init() {
	gob.Register(Float(0))
	gob.Register(Int(0))
	gob.Register(String(""))
	gob.Register(Bool(false))
	gob.Register(List{})
	gob.Register(&Map{})
}

// Kind represents the kind of a value.
type Kind int

const (
	Integer Kind = iota
	Real
	Str
	Seq
	ValueDict
	Boolean
)

func (k Kind) String() string {
	switch k {
	case Integer:
		return "integer"
	case Real:
		return "real"
	case Str:
		return "string"
	case Seq:
		return "seq"
	case ValueDict:
		return "valuedict"
	case Boolean:
		return "boolean"
	default:
		panic(k)
	}
}

// Value is the type of parameter values. Values must be
// directly comparable.
type Value interface {
	// String returns a textual description of the parameter value.
	String() string

	// Kind returns the kind of this value.
	Kind() Kind

	// Equal tells whether two values are equal. Values of different
	// kinds are never equal.
	Equal(Value) bool

	// Less returns true if the value is less than the provided value.
	// Less is defined only for values of the same type.
	Less(Value) bool

	// Float returns the floating point value of float-typed values.
	Float() float64

	// Int returns the integer value of integer-typed values.
	Int() int64

	// Str returns the string of string-typed values.
	Str() string

	// Bool returns the boolean of boolean-typed values.
	Bool() bool

	// Len returns the length of a sequence value.
	Len() int

	// Index returns the value at an index of a sequence.
	Index(i int) Value

	// Hash adds the value's hask to the provided hasher.
	Hash(h hash.Hash)
}

// Zero returns the zero value for the provided kind.
func Zero(kind Kind) Value {
	switch kind {
	default:
		panic(kind)
	case Integer:
		return Int(0)
	case Real:
		return Float(0)
	case Str:
		return String("")
	case ValueDict:
		return new(Values)
	case Boolean:
		return Bool(false)
	case Seq:
		return new(List)
	}
}

// Int is an integer-typed value.
type Int int64

// String implements Value.
func (v Int) String() string { return fmt.Sprint(int64(v)) }

// Kind implements Value.
func (Int) Kind() Kind { return Integer }

func (v Int) Equal(w Value) bool {
	if w.Kind() != Integer {
		return false
	}
	return v.Int() == w.Int()
}

// Less implements Value.
func (v Int) Less(w Value) bool {
	return v.Int() < w.Int()
}

// Float implements Value.
func (Int) Float() float64 { panic("Float on Int") }

// Str implements Value.
func (Int) Str() string { panic("Str on Int") }

// Int implements Value.
func (v Int) Int() int64 { return int64(v) }

func (Int) Bool() bool { panic("Bool on Int") }

func (Int) Len() int                            { panic("Len on Int") }
func (Int) Index(int) Value                     { panic("Index on Int") }
func (Int) Put(key string, value Value)         { panic("Put on Int") }
func (Int) Get(key string) Value                { panic("Get on Int") }
func (Int) Range(func(key string, value Value)) { panic("Range on Int") }

func (v Int) Hash(h hash.Hash) {
	writehash.Uint64(h, uint64(v))
}

// Float is a float-typed value.
type Float float64

// String implements Value.
func (v Float) String() string { return fmt.Sprint(float64(v)) }

// Kind implements Value.
func (Float) Kind() Kind { return Real }

func (v Float) Equal(w Value) bool {
	if w.Kind() != Real {
		return false
	}
	return v.Float() == w.Float()
}

// Less implements Value.
func (v Float) Less(w Value) bool {
	return v.Float() < w.Float()
}

// Float implements Value.
func (v Float) Float() float64 { return float64(v) }

// Str implements Value.
func (Float) Str() string { panic("Str on Float") }

// Int implements Value.
func (Float) Int() int64 { panic("Int on Float") }

func (Float) Bool() bool { panic("Bool on Float") }

func (Float) Len() int                            { panic("Len on Float") }
func (Float) Index(int) Value                     { panic("Index on Float") }
func (Float) Put(key string, value Value)         { panic("Put on Float") }
func (Float) Get(key string) Value                { panic("Get on Float") }
func (Float) Range(func(key string, value Value)) { panic("Range on Float") }

func (v Float) Hash(h hash.Hash) {
	writehash.Float64(h, v.Float())
}

// String is a string-typed value.
type String string

func (v String) Equal(w Value) bool {
	if w.Kind() != Str {
		return false
	}
	return v.Str() == w.Str()
}

// Less implements Value.
func (v String) Less(w Value) bool {
	return v.Str() < w.Str()
}

// String implements Value.
func (v String) String() string { return string(v) }

// Kind implements Value.
func (String) Kind() Kind { return Str }

// Float implements Value.
func (String) Float() float64 { panic("Float on String") }

// Int implements Value.
func (String) Int() int64 { panic("Int on String") }

func (String) Bool() bool { panic("Bool on String") }

// Str implements Value.
func (v String) Str() string { return string(v) }

func (String) Len() int                            { panic("Len on String") }
func (String) Index(int) Value                     { panic("Index on String") }
func (String) Put(key string, value Value)         { panic("Put on String") }
func (String) Get(key string) Value                { panic("Get on String") }
func (String) Range(func(key string, value Value)) { panic("Range on String") }

func (v String) Hash(h hash.Hash) {
	writehash.String(h, v.Str())
}

// Bool is a boolean-typed value.
type Bool bool

// String implements Value.
func (v Bool) String() string { return fmt.Sprint(bool(v)) }

// Kind implements Value.
func (Bool) Kind() Kind { return Boolean }

func (v Bool) Equal(w Value) bool {
	if w.Kind() != Boolean {
		return false
	}
	return v.Bool() == w.Bool()
}

// Less implements Value.
func (v Bool) Less(w Value) bool {
	return !v.Bool() && w.Bool()
}

// Bool implements Value.
func (v Bool) Float() float64 { panic("Float on Bool") }

// Str implements Value.
func (Bool) Str() string { panic("Str on Bool") }

// Int implements Value.
func (Bool) Int() int64 { panic("Int on Bool") }

// Bool implements Value.
func (v Bool) Bool() bool { return bool(v) }

func (Bool) Len() int                            { panic("Len on Bool") }
func (Bool) Index(int) Value                     { panic("Index on Bool") }
func (Bool) Put(key string, value Value)         { panic("Put on Bool") }
func (Bool) Get(key string) Value                { panic("Get on Bool") }
func (Bool) Range(func(key string, value Value)) { panic("Range on Bool") }

func (v Bool) Hash(h hash.Hash) {
	writehash.Bool(h, bool(v))
}

// List is a list-typed value.
type List []Value

func (l List) Equal(m Value) bool {
	if m.Kind() != Seq {
		return false
	}
	if l.Len() != m.Len() {
		return false
	}
	for i := 0; i < l.Len(); i++ {
		if !l.Index(i).Equal(m.Index(i)) {
			return false
		}
	}
	return true
}

// Less implements Value.
func (l List) Less(m Value) bool {
	for i := 0; i < l.Len(); i++ {
		if m.Len() <= i {
			break
		}
		if l.Index(i).Less(m.Index(i)) {
			return true
		} else if m.Index(i).Less(l.Index(i)) {
			return false
		}
	}
	return l.Len() < m.Len()
}

// String implements Value.
func (l List) String() string {
	elems := make([]string, l.Len())
	for i := range elems {
		elems[i] = l.Index(i).String()
	}
	return "[" + strings.Join(elems, ", ") + "]"
}

// Kind implements Value.
func (List) Kind() Kind { return Seq }

// Float implements Value.
func (List) Float() float64 { panic("Float on List") }

// Int implements Value.
func (List) Int() int64 { panic("Int on List") }

// Str implements Value.
func (List) Str() string { panic("Str on List") }

func (List) Bool() bool { panic("Bool on List") }

// Len returns the length of the list.
func (l List) Len() int { return len(l) }

// Index returns the ith element of the list.
func (l List) Index(i int) Value { return l[i] }

func (List) Put(key string, value Value)         { panic("Put on *List") }
func (List) Get(key string) Value                { panic("Get on *List") }
func (List) Range(func(key string, value Value)) { panic("Range on *List") }

func (l List) Hash(h hash.Hash) {
	for _, v := range l {
		v.Hash(h)
	}
}

// A NamedValue is a value that is assigned a name.
type NamedValue struct {
	Name string
	Value
}

// Values is a sorted set of named values, used as a concrete
// instantiation of a set of parameters.
//
// Note: the representation of Values is not optimal. Ideally we
// would store it as a sorted list of NamedValues. However, backwards
// compatibility (by way of gob) forces our hand here.
type Values map[string]Value

// String returns a (stable) textual description of the value set.
func (v Values) String() string {
	elems := make([]string, v.Len())
	for i, v := range v.Sorted() {
		elems[i] = fmt.Sprintf("%s=%s", v.Name, v.Value)
	}
	return strings.Join(elems, ",")
}

func (Values) Kind() Kind { return ValueDict }

func (v Values) Equal(wv Value) bool {
	w, ok := wv.(Values)
	if !ok {
		return false
	}
	if v.Len() != w.Len() {
		return false
	}
	vlist, wlist := v.Sorted(), w.Sorted()
	for i := range vlist {
		if vlist[i].Name != wlist[i].Name {
			return false
		}
		if !vlist[i].Equal(wlist[i]) {
			return false
		}
	}
	return true
}

func (Values) Less(Value) bool { panic("Less on Values") }
func (Values) Float() float64  { panic("Float on Values") }
func (Values) Int() int64      { panic("Int on Values") }
func (Values) Str() string     { panic("Str on Values") }
func (Values) Bool() bool      { panic("Bool on Values") }

func (v Values) Len() int { return len(v) }

func (Values) Index(i int) Value { panic("Index on Values") }

func (v Values) Hash(h hash.Hash) {
	for _, v := range v.Sorted() {
		String(v.Name).Hash(h)
		v.Value.Hash(h)
	}
}

func (v Values) Sorted() []NamedValue {
	vals := make([]NamedValue, 0, len(v))
	for k, v := range v {
		vals = append(vals, NamedValue{k, v})
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i].Name < vals[j].Name })
	return vals
}

// Hash returns a 64-bit hash for the value v.
func Hash(v Value) uint64 {
	h := fnv.New64a()
	v.Hash(h)
	return h.Sum64()
}
