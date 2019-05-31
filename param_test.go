// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/grailbio/diviner"
)

func TestDiscrete(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	vals := []diviner.Value{diviner.String("x"), diviner.String("y"), diviner.String("z")}
	d := diviner.NewDiscrete(vals...)
	const N = 1000
	for i := 0; i < N; i++ {
		val := d.Sample(rng)
		switch val.Str() {
		case "x", "y", "z":
		default:
			t.Error("invalid value", val)
		}
	}
}

func TestRange(t *testing.T) {
	const (
		beg = 0.2
		end = 23.4
		N   = 10000
	)
	// Naive test for uniformity.
	var (
		rng     = rand.New(rand.NewSource(0))
		r       = diviner.NewRange(diviner.Float(beg), diviner.Float(end))
		sum     float64
		samples = make([]float64, N)
	)
	for i := range samples {
		v := r.Sample(rng).Float()
		samples[i] = v
		if v < beg || v >= end {
			t.Errorf("invalid value %f", v)
		}
		sum += v
	}
	var (
		mean       = sum / N
		sumSquares float64
	)
	for _, v := range samples {
		sumSquares += (v - mean) * (v - mean)
	}
	stddev := math.Sqrt(sumSquares / (N - 1))
	if stddev < 6 {
		t.Errorf("stddev %f too low", stddev)
	}
	if mean < (end-beg)/2-1 || mean > (end-beg)/2+1 {
		t.Errorf("mean %f out of range", mean)
	}
}

func TestIsValid(t *testing.T) {
	tests := []struct {
		params diviner.Params
		values diviner.Values
		result bool
	}{
		{
			params: diviner.Params{
				"a": diviner.NewDiscrete(diviner.Float(0), diviner.Float(1), diviner.Float(2)),
				"b": diviner.NewDiscrete(diviner.Int(7), diviner.Int(8)),
			},
			values: diviner.Values{
				"a": diviner.Float(1),
			},
			result: false,
		},
		{
			params: diviner.Params{
				"a": diviner.NewDiscrete(diviner.Float(0), diviner.Float(1), diviner.Float(2)),
				"b": diviner.NewDiscrete(diviner.Int(7), diviner.Int(8)),
			},
			values: diviner.Values{
				"a": diviner.Float(1),
				"b": diviner.Int(7),
			},
			result: true,
		},
		{
			params: diviner.Params{
				"a": diviner.NewDiscrete(diviner.Float(0), diviner.Float(1), diviner.Float(2)),
				"b": diviner.NewDiscrete(diviner.Int(7), diviner.Int(8)),
			},
			values: diviner.Values{
				"a": diviner.Float(1),
				"b": diviner.Int(9),
			},
			result: false,
		},
		{
			params: diviner.Params{
				"a": diviner.NewRange(diviner.Int(7), diviner.Int(100)),
			},
			values: diviner.Values{
				"a": diviner.Int(99),
			},
			result: true,
		},
		{
			params: diviner.Params{
				"a": diviner.NewRange(diviner.Int(7), diviner.Int(100)),
			},
			values: diviner.Values{
				"a": diviner.Int(100),
			},
			result: false,
		},
		{
			params: diviner.Params{
				"a": diviner.NewRange(diviner.Int(7), diviner.Int(100)),
			},
			values: diviner.Values{
				"a": diviner.Int(99),
				"b": diviner.Int(0),
			},
			result: false,
		},
	}

	for _, test := range tests {
		if test.params.IsValid(test.values) != test.result {
			t.Errorf("Wrong result for %v, %v: want %v", test.params, test.values, test.result)
		}
	}
}
