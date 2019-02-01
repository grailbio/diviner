// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package diviner implements common data structures to support a
// black-box optimization framework in the style of Google Vizier
// [1]. Implementations of optimization algorithms as well runner
// infrastructure, parallel execution, and a command line tool are
// implemented in subpackages.
//
// [1] http://www.kdd.org/kdd2017/papers/view/google-vizier-a-service-for-black-box-optimization
package diviner

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"go.starlark.net/starlark"
)

var errNotHashable = errors.New("value not hashable")

// Params stores a set of parameters under optimization.
type Params map[string]Param

// NamedParam represents a named parameter.
type NamedParam struct {
	// Name is the parameter's name.
	Name string
	// Param is the parameter.
	Param
}

// Sorted returns the set of parameters sorted by name.
func (p Params) Sorted() []NamedParam {
	keys := make([]string, 0, len(p))
	for key := range p {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	sorted := make([]NamedParam, len(p))
	for i, key := range keys {
		sorted[i] = NamedParam{key, p[key]}
	}
	return sorted
}

// Metrics is a set of measurements output by black boxes. A subset
// of metrics may be used in the optimization objective, but the set
// of metrics may include others for diagnostic purposes.
type Metrics map[string]float64

// A Trial is the result of a single run of a black box.
type Trial struct {
	// Values is the set of parameter values used for the run.
	Values Values
	// Metrics is the metrics produced by the black box during
	// the run.
	Metrics Metrics
}

// Direction is the direction of the objective.
type Direction int

const (
	// Minimize indicates that the objective is to minimize a metric.
	Minimize Direction = iota
	// Maximize indicates that the objective is to maximize a metric.
	Maximize
)

// String returns a textual representation of the objective direction d.
func (d Direction) String() string {
	switch d {
	case Minimize:
		return "minimize"
	case Maximize:
		return "maximize"
	default:
		panic(d)
	}
}

// Objective is an optimization objective.
type Objective struct {
	// Direction indicates the direction (minimize, maximize) of the
	// the optimization objective.
	Direction Direction
	// Metric names the metric to be optimized.
	Metric string
}

// String returns a textual description of the optimization objective.
func (o Objective) String() string {
	return fmt.Sprintf("%s(%s)", o.Direction, o.Metric)
}

// Type implements starlark.Value.
func (Objective) Type() string { return "objective" }

// Freeze implements starlark.Value.
func (Objective) Freeze() {}

// Truth implements starlark.Value.
func (Objective) Truth() starlark.Bool { return true }

// Hash implements starlark.Value.
func (Objective) Hash() (uint32, error) { return 0, errNotHashable }

// An Oracle is an optimization algorithm that picks a set of
// parameter values given a history of runs.
type Oracle interface {
	// Next returns the next n parameter values to run, given the
	// provided history of trials, the set of parameters and an
	// objective. If Next returns fewer than n trials, then the oracle
	// is exhausted.
	Next(previous []Trial, params Params, objective Objective, n int) ([]Values, error)

	// TODO(marius): add methods for validating whether a Params is compatible
	// with the oracle.
}

// A RunConfig describes how to perform a single run of a black box
// with a set of parameter values. Runs are defined by a bash script
// and a set of files that must be included in its run environment.
//
// Black boxes emit metrics by printing to standard output lines that
// begin with "METRICS: ", followed by a set of comma-separated
// key-value pairs of metric values. Each metric must be a number.
// For example, the following line emits metrics for "acc" and "loss":
//
// 	METRICS: acc=0.55,loss=12.3
//
// TODO(marius): make this mechanism more flexible and
// less error prone.
//
// TODO(marius): allow interpreters other than Bash.
type RunConfig struct {
	// Script is a script that should be interpreted by Bash.
	Script string
	// LocalFiles is a set of files (local to where diviner is run)
	// that should be made available in the script's environment.
	// These files are copied into the script's working directory,
	// retaining their basenames. (Thus the set of basenames in
	// the list should not collide.)
	LocalFiles []string
}

// String returns a textual description of the run config.
func (c RunConfig) String() string {
	return fmt.Sprintf("run_config(script=%q, local_files=[%s])", c.Script, strings.Join(c.LocalFiles, ", "))
}

// Type implements starlark.Value.
func (RunConfig) Type() string { return "run_config" }

// Freeze implements starlark.Value.
func (RunConfig) Freeze() {}

// Truth implements starlark.Value.
func (RunConfig) Truth() starlark.Bool { return true }

// Hash implements starlark.Value.
func (RunConfig) Hash() (uint32, error) { return 0, errors.New("run_config is not hashable") }

// A Study is an experiment comprising a black box, optimization
// objective, and an oracle responsible for generating trials.
type Study struct {
	// Name is the name of the study.
	Name string
	// Objective is the objective to be maximized.
	Objective Objective
	// Oracle is the oracle used to pick parameter values.
	Oracle Oracle
	// Params is the set of parameters accepted by this
	// study.
	Params Params
	// Run is called with a set of Values (i.e., a concrete
	// instantiation of values in the ranges as indicated by the black
	// box parameters defined above); it produces a run configuration
	// which is then used to conduct a trial of these parameter values.
	Run func(Values) RunConfig
}

// String returns a textual description of the study.
func (s Study) String() string {
	return fmt.Sprintf("study(name=%s, params=%s, objective=%s)", s.Name, s.Params, s.Objective)
}

// Type implements starlark.Value.
func (Study) Type() string { return "study" }

// Freeze implements starlark.Value.
func (Study) Freeze() {}

// Truth implements starlark.Value.
func (Study) Truth() starlark.Bool { return true }

// Hash implements starlark.Value.
func (Study) Hash() (uint32, error) { return 0, errNotHashable }
