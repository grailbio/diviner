// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package oracle includes pre-defined oracles for picking parameter
// values.
package oracle

import (
	"encoding/gob"
	"fmt"
	"sort"

	"github.com/grailbio/diviner"
)

// Func is an adapter type so that ordinary functions may be used as oracles.
type Func func(previous []diviner.Trial, params diviner.Params, objective diviner.Objective, howmany int) ([]diviner.Values, error)

// Next implements diviner.Oracle.
func (f Func) Next(previous []diviner.Trial, params diviner.Params, objective diviner.Objective, howmany int) ([]diviner.Values, error) {
	return f(previous, params, objective, howmany)
}

func init() {
	gob.Register(&GridSearch{})
}

type GridSearch struct{}

// GridSearch is an oracle that performs grid searching [1]. It currently
// supports only discrete parameters, and returns errors if continuous
// parameters are encountered. GridSearch is deterministic, always
// returning parameters in the same order.
//
// [1] https://en.wikipedia.org/wiki/Hyperparameter_optimization
func (_ *GridSearch) Next(previous []diviner.Trial,
	params diviner.Params, objective diviner.Objective,
	howmany int) ([]diviner.Values, error) {
	var (
		keys    = make([]string, 0, len(params))
		pvalues = make(map[string][]diviner.Value)
		n       = 1
	)
	for key := range params {
		pvalues[key] = params[key].Values()
		if len(pvalues[key]) == 0 {
			return nil, fmt.Errorf("parameter %s is not discrete", key)
		}
		n *= len(pvalues[key])
		keys = append(keys, key)
	}
	total := n
	if howmany > 0 && n > howmany {
		n = howmany
	}
	sort.Strings(keys)

	// We map each point in the search space to an integer by treating
	// each parameter as a digit with the base of the cardinality of
	// parameters of that kind. We then want to generate missing
	// integers, and map them back into parameter space. This allows us
	// to cheaply check whether a set of parameter values have already
	// been tried.
	digits := make(map[string]map[diviner.Value]int)
	for key, values := range pvalues {
		digits[key] = make(map[diviner.Value]int)
		for i, v := range values {
			digits[key][v] = i
		}
	}

	done := make([]bool, total)
	for _, trial := range previous {
		var (
			m   = 1
			num int
		)
		for _, key := range keys {
			digit, ok := digits[key][trial.Values[key]]
			if !ok {
				panic(key)
			}
			num += m * digit
			m *= len(pvalues[key])
		}
		done[num] = true
	}

	values := make([]diviner.Values, 0, n)
	for i, ok := range done {
		if ok {
			continue
		}
		var (
			m  = 1
			vs = make(diviner.Values)
		)
		for _, key := range keys {
			digit := (i / m) % len(pvalues[key])
			m *= len(pvalues[key])
			vs[key] = pvalues[key][digit]
		}
		values = append(values, vs)
		if len(values) == n {
			break
		}
	}
	return values, nil
}
