// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package oracle_test

import (
	"flag"
	"fmt"
	"math"
	"testing"

	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/oracle"
)

// Disable this by default because test machines may not
// have python+scikit-optimize.
var testSkopt = flag.Bool("skopt", false, "test skopt optimizer")

func TestSkoptOracle(t *testing.T) {
	if !*testSkopt {
		t.Skip("-skopt=false")
	}
	params := diviner.Params{
		"x":  diviner.NewDiscrete(diviner.Float(0), diviner.Float(1), diviner.Float(2)),
		"y":  diviner.NewRange(diviner.Int(-10), diviner.Int(20)),
		"z":  diviner.NewDiscrete(diviner.String("a"), diviner.String("b")),
		"zz": diviner.NewRange(diviner.Float(0), diviner.Float(0.5)),
	}
	var o oracle.Skopt
	values, err := o.Next(nil, params, diviner.Objective{diviner.Maximize, "acc"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(values)
}

func TestSkoptOracleOptim(t *testing.T) {
	if !*testSkopt {
		t.Skip("-skopt=false")
	}
	// (-.29 derived by eyeing it in Grapher.app)
	testOracle(t, new(oracle.Skopt), diviner.Minimize, -.29, func(x float64) float64 {
		return math.Sin(5*x) * (1 - math.Tanh(x*x))
	})
}
