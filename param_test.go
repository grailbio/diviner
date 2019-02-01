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
		r       = diviner.NewRange(beg, end)
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
