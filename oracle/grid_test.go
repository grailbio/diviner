// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package oracle_test

import (
	"reflect"
	"testing"

	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/oracle"
)

func TestGridSearch(t *testing.T) {
	params := diviner.Params{
		"x": diviner.NewDiscrete(diviner.Float(0), diviner.Float(1), diviner.Float(2)),
		"y": diviner.NewDiscrete(diviner.Float(10), diviner.Float(20), diviner.Float(30)),
		"z": diviner.NewDiscrete(diviner.String("a"), diviner.String("b")),
	}

	gs := &oracle.GridSearch{}
	values, err := gs.Next(nil, params, diviner.Objective{}, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(values), 3*3*2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	diviner.SortValues(values)
	var index int
	for i, x := range []float64{0, 1, 2} {
		for j, y := range []float64{10, 20, 30} {
			for k, z := range []string{"a", "b"} {
				vs := values[index]
				if got, want := vs["x"].Float(), x; got != want {
					t.Errorf("(%d, %d, %d, x) got %v, want %v", i, j, k, got, want)
				}
				if got, want := vs["y"].Float(), y; got != want {
					t.Errorf("(%d, %d, %d, y) got %v, want %v", i, j, k, got, want)
				}
				if got, want := vs["z"].Str(), z; got != want {
					t.Errorf("(%d, %d, %d, z %d) got %v, want %v", i, j, k, index, got, want)
				}
				index++
			}
		}
	}

	// Make sure we don't repeat trials already performed.
	trials := make([]diviner.Trial, len(values))
	for i := range trials {
		trials[i].Values = values[i]
	}
	next, err := gs.Next(trials, params, diviner.Objective{}, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(next), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	next, err = gs.Next(trials[3:], params, diviner.Objective{}, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(next), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	diviner.SortValues(next)
	for i, vs := range next {
		if got, want := vs, values[i]; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
