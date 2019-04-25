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

// A Metric is a single, named metric.
type Metric struct {
	Name  string
	Value float64
}

// Metrics is a set of measurements output by black boxes. A subset
// of metrics may be used in the optimization objective, but the set
// of metrics may include others for diagnostic purposes.
type Metrics map[string]float64

// Sorted returns the metrics in m sorted by name.
func (m Metrics) Sorted() []Metric {
	metrics := make([]Metric, 0, len(m))
	for k, v := range m {
		metrics = append(metrics, Metric{k, v})
	}
	sort.Slice(metrics, func(i, j int) bool { return metrics[i].Name < metrics[j].Name })
	return metrics
}

func (m Metrics) Equal(n Metrics) bool {
	if len(m) != len(n) {
		return false
	}
	for k, mv := range m {
		nv, ok := n[k]
		if !ok {
			return false
		}
		if mv != nv {
			return false
		}
	}
	return true
}

// Merge merges metrics n into m; values in n overwrite values in m.
func (m *Metrics) Merge(n Metrics) {
	if *m == nil {
		*m = make(Metrics)
	}
	for k, v := range n {
		(*m)[k] = v
	}
}

// A Trial is the result of a single run of a black box.
type Trial struct {
	// Values is the set of parameter values used for the run.
	Values Values
	// Metrics is the metrics produced by the black box during
	// the run.
	Metrics Metrics
	// Pending indicates whether this is a pending trial. Pending trials
	// may have incomplete or non-final metrics.
	Pending bool
}

// Equal reports whether the two trials are equal.
func (t Trial) Equal(u Trial) bool {
	return t.Values.Equal(u.Values) && t.Metrics.Equal(u.Metrics)
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

// Arrow returns a decorative arrow indicating the direction of d.
func (d Direction) Arrow() string {
	switch d {
	case Maximize:
		return "↑"
	case Minimize:
		return "↓"
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

// A Dataset describes a preprocessing step that's required
// by a run. It may be shared among multiple runs.
type Dataset struct {
	// Name is a unique name describing the dataset. Uniqueness is
	// required: diviner only runs one dataset for each named dataset,
	Name string
	// IfNotExist may contain a URL which is checked for existence
	// before running the script that produces this dataset. It is
	// assumed the dataset already exists if the URL exists.
	IfNotExist string
	// LocalFiles is a set of files (local to where diviner is run)
	// that should be made available in the script's environment.
	// These files are copied into the script's working directory,
	// retaining their basenames. (Thus the set of basenames in
	// the list should not collide.)
	LocalFiles []string
	// Script is a Bash script that is run to produce this dataset.
	Script string

	// Systems identifies the list of systems where the dataset run should be
	// performed. This can be used to schedule jobs with different kinds of
	// systems requirements. If Len(Systems)>1, each is tried until one of them
	// successfully allocates a machine.
	Systems []*System
}

// String returns a textual description of the dataset.
func (d Dataset) String() string { return fmt.Sprintf("dataset(%s)", d.Name) }

// Type implements starlark.Value.
func (Dataset) Type() string { return "dataset" }

// Freeze implements starlark.Value.
func (Dataset) Freeze() {}

// Truth implements starlark.Value.
func (Dataset) Truth() starlark.Bool { return true }

// Hash implements starlark.Value.
func (Dataset) Hash() (uint32, error) { return 0, errNotHashable }

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
	// Datasets contains the set of datasets required by this
	// run.
	Datasets []Dataset
	// Script is a script that should be interpreted by Bash.
	Script string
	// LocalFiles is a set of files (local to where diviner is run)
	// that should be made available in the script's environment.
	// These files are copied into the script's working directory,
	// retaining their basenames. (Thus the set of basenames in
	// the list should not collide.)
	LocalFiles []string

	// Systems identifies the list of systems where the run should be
	// performed. This can be used to schedule jobs with different kinds of
	// systems requirements. If Len(Systems)>1, each is tried until one of them
	// successfully allocates a machine.
	Systems []*System
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
	// Params is the set of parameters accepted by this
	// study.
	Params Params

	// Oracle is the oracle used to pick parameter values.
	Oracle Oracle `json:"-"` // TODO(marius): encode oracle name/type/params?
	// Run is called with a set of Values (i.e., a concrete
	// instantiation of values in the ranges as indicated by the black
	// box parameters defined above); it produces a run configuration
	// which is then used to conduct a trial of these parameter values.
	// Parameter id is a unique id for the run (vis-a-vis diviner's
	// database). It may be used to name data and other external
	// resources associated with the run.
	Run func(vals Values, id string) (RunConfig, error) `json:"-"`
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
