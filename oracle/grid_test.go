// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package oracle_test

import (
	"reflect"
	"sort"
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
	sortValues(values)
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
	sortValues(next)
	for i, vs := range next {
		if got, want := vs, values[i]; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestGridSearchRange(t *testing.T) {
	params := diviner.Params{
		"x": diviner.NewRange(diviner.Int(0), diviner.Int(100)),
	}
	var search oracle.GridSearch
	values, err := search.Next(nil, params, diviner.Objective{}, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(values), 100; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	sortValues(values)
	for i, v := range values {
		if got, want := len(v), 1; got != want {
			t.Errorf("got %v, want %v", got, want)
			continue
		}
		if got, want := v["x"].Int(), int64(i); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestGridSearchList(t *testing.T) {
	params := diviner.Params{
		"x": diviner.NewDiscrete(diviner.List{}, diviner.List{diviner.Int(1)}),
	}
	var search oracle.GridSearch
	values, err := search.Next(nil, params, diviner.Objective{}, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(values), 2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	sortValues(values)
	if got, want := values[0]["x"].Len(), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := values[1]["x"].Len(), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Take out a value and make sure we get the right one back.
	values, err = search.Next([]diviner.Trial{{Values: values[0]}}, params, diviner.Objective{}, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(values), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := values[0]["x"].Len(), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// sortValues sorts the provided set of values by keys. It assumes
// that all of the values have exactly the same sets of keys.
func sortValues(vs []diviner.Values) {
	if len(vs) == 0 {
		return
	}
	keys := make([]string, 0, len(vs[0]))
	for key := range vs[0] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	sort.SliceStable(vs, func(i, j int) bool {
		for _, k := range keys {
			switch {
			case vs[i][k].Less(vs[j][k]):
				return true
			case vs[j][k].Less(vs[i][k]):
				return false
			}
		}
		return false
	})
}
