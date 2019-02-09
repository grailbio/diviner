// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package oracle_test

import (
	"testing"

	"github.com/grailbio/diviner"
)

const ε = 0.1

func testOracle(t *testing.T, o diviner.Oracle, dir diviner.Direction, x float64, fn func(float64) float64) {
	if testing.Short() {
		t.Skip("expensive")
	}
	t.Helper()
	const N = 3
	var (
		params    = diviner.Params{"x": diviner.NewRange(diviner.Float(-2), diviner.Float(2))}
		objective = diviner.Objective{dir, "y"}
		trials    []diviner.Trial
	)
	for i := 0; i < 5; i++ {
		values, err := o.Next(trials, params, objective, N)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range values {
			trials = append(trials, diviner.Trial{v, diviner.Metrics{"y": fn(v["x"].Float())}})
		}
	}
	// Find the best value. It should be close to approx.
	best := trials[0].Metrics["y"]
	for _, trial := range trials {
		if y := trial.Metrics["y"]; dir == diviner.Minimize && y < best || dir == diviner.Maximize && best < y {
			best = y
		}
	}

	if approx := fn(x); best < approx-ε || best > approx+ε {
		t.Errorf("best value %f is not within expected value f(%f)=%f", best, x, approx)
	}
}
