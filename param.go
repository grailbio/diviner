// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner

import (
	"fmt"
	"math/rand"
	"strings"

	"go.starlark.net/starlark"
)

// A Param is a kind of parameter. Params determine the range of
// allowable values of an input.
type Param interface {
	// Kind returns the kind of values encapsulated by this param.
	Kind() Kind

	// Values returns the set of values allowable by this parameter.
	// Nil is returned if the param's image is not finite.
	Values() []Value

	// Sample returns a Value from this param sampled by the provided
	// random number generator.
	Sample(r *rand.Rand) Value

	// Params implement starlark.Value so they can be represented
	// directly in starlark configuration scripts.
	starlark.Value
}

// A Discrete is a parameter that takes on a finite set of values.
type Discrete struct {
	values []Value
	kind   Kind
}

// NewDiscrete returns a new discrete param comprising the
// given values. NewDiscrete panics if all returned values are
// not of the same Kind, or if zero values are passed.
func NewDiscrete(values ...Value) *Discrete {
	if len(values) == 0 {
		panic("diviner.NewDiscrete: no values passed")
	}
	kind := values[0].Kind()
	for _, v := range values {
		if v.Kind() != kind {
			panic(fmt.Sprintf("diviner.NewDiscrete: mixed kinds: %s and %s", v.Kind(), kind))
		}
	}
	return &Discrete{values: values, kind: kind}
}

// String returns a description of this parameter.
func (d *Discrete) String() string {
	vals := make([]string, len(d.values))
	for i := range vals {
		vals[i] = d.values[i].String()
	}
	return fmt.Sprintf("discrete(%s)", strings.Join(vals, ", "))
}

// Kind returns the kind of values represented by this discrete param.
func (d *Discrete) Kind() Kind {
	return d.kind
}

// Values returns the possible values of the discrete param in
// the order given.
func (d *Discrete) Values() []Value {
	return d.values
}

// Sample draws a value set of parameter values and returns it.
func (d *Discrete) Sample(r *rand.Rand) Value {
	return d.values[r.Intn(len(d.values))]
}

// Type implements starlark.Value.
func (*Discrete) Type() string { return "discrete" }

// Freeze implements starlark.Value.
func (*Discrete) Freeze() {}

// Truth implements starlark.Value.
func (*Discrete) Truth() starlark.Bool { return true }

// Hash implements starlark.Value.
func (*Discrete) Hash() (uint32, error) { return 0, errNotHashable }

// Range is a parameter that is defined over a range of
// real numbers.
type Range struct {
	start, end float64
}

// NewRange returns a range parameter representing the
// range of values [start, end).
func NewRange(start, end float64) *Range {
	if end < start {
		panic("invalid range")
	}
	return &Range{start: start, end: end}
}

// String returns a description of this range parameter.
func (r *Range) String() string {
	return fmt.Sprintf("range(%f, %f)", r.start, r.end)
}

// Kind returns Real.
func (*Range) Kind() Kind { return Real }

// Values returns nil: Ranges represent infinite sets of values.
func (r *Range) Values() []Value { return nil }

// Sample draws a random sample from within the range represented by
// this parameter.
func (r *Range) Sample(rnd *rand.Rand) Value {
	return Float(r.start + rnd.Float64()*(r.end-r.start))
}

// Type implements starlark.Value.
func (*Range) Type() string { return "range" }

// Freeze implements starlark.Value.
func (*Range) Freeze() {}

// Truth implements starlark.Value.
func (*Range) Truth() starlark.Bool { return true }

// Hash implements starlark.Value.
func (*Range) Hash() (uint32, error) { return 0, errNotHashable }
