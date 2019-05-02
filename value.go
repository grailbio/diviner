// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner

import (
	"encoding/gob"
	"fmt"
	"sort"
	"strings"
)

func init() {
	gob.Register(Float(0))
	gob.Register(Int(0))
	gob.Register(String(""))
	gob.Register(&List{})
}

// Kind represents the kind of a value.
type Kind int

const (
	Integer Kind = iota
	Real
	Str
	Seq
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

	// Less returns true if the value is less than the provided value.
	// Less is defined only for values of the same type.
	Less(Value) bool

	// Float returns the floating point value of float-typed values.
	Float() float64

	// Int returns the integer value of integer-typed values.
	Int() int64

	// Str returns the string of string-typed values.
	Str() string

	// Len returns the length of a sequence value.
	Len() int

	// Index returns the value at an index of a sequence.
	Index(i int) Value
}

// Int is an integer-typed value.
type Int int64

// String implements Value.
func (v Int) String() string { return fmt.Sprint(int64(v)) }

// Kind implements Value.
func (Int) Kind() Kind { return Integer }

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

func (Int) Len() int        { panic("Len on Int") }
func (Int) Index(int) Value { panic("Index on Int") }

// Float is a float-typed value.
type Float float64

// String implements Value.
func (v Float) String() string { return fmt.Sprint(float64(v)) }

// Kind implements Value.
func (Float) Kind() Kind { return Real }

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

func (Float) Len() int        { panic("Len on Float") }
func (Float) Index(int) Value { panic("Index on Float") }

// String is a string-typed value.
type String string

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

// Str implements Value.
func (v String) Str() string { return string(v) }

func (String) Len() int        { panic("Len on String") }
func (String) Index(int) Value { panic("Index on String") }

// *List is a list-typed value.
type List []Value

// Less implements Value.
func (l *List) Less(m Value) bool {
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
func (l *List) String() string {
	elems := make([]string, l.Len())
	for i := range elems {
		elems[i] = l.Index(i).String()
	}
	return "[" + strings.Join(elems, ", ") + "]"
}

// Kind implements Value.
func (*List) Kind() Kind { return Seq }

// Float implements Value.
func (*List) Float() float64 { panic("Float on List") }

// Int implements Value.
func (*List) Int() int64 { panic("Int on List") }

// Str implements Value.
func (*List) Str() string { panic("Str on List") }

// Len returns the length of the list.
func (l *List) Len() int { return len(*l) }

// Index returns the ith element of the list.
func (l *List) Index(i int) Value { return (*l)[i] }

// A NamedValue is a value that is assigned a name.
type NamedValue struct {
	Name string
	Value
}

// Values is a set of named value, used as a concrete instantiation
// of a set of parameters.
type Values map[string]Value

// Sorted returns the values in v sorted by name.
func (v Values) Sorted() []NamedValue {
	vals := make([]NamedValue, 0, len(v))
	for k, v := range v {
		vals = append(vals, NamedValue{k, v})
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i].Name < vals[j].Name })
	return vals
}

func (v Values) Equal(w Values) bool {
	if len(v) != len(w) {
		return false
	}
	for k, vv := range v {
		wv, ok := w[k]
		if !ok {
			return false
		}
		if vv.Kind() != wv.Kind() {
			return false
		}
		if vv.Less(wv) || wv.Less(vv) {
			return false
		}
	}
	return true
}

// String returns a (stable) textual description of the value set.
func (v Values) String() string {
	keys := make([]string, 0, len(v))
	for key := range v {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	elems := make([]string, len(keys))
	for i, key := range keys {
		elems[i] = fmt.Sprintf("%s=%s", key, v[key])
	}
	return strings.Join(elems, ",")
}
