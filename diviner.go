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
	"log"
	"math/bits"
	"sort"
	"strings"
	"time"

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

// IsValid returns whether the given set of values are a valid assignment
// of exactly the parameters in this Params.
func (p Params) IsValid(values Values) bool {
	if len(p) != len(values) {
		return false
	}
	for name, v := range values {
		param, ok := p[name]
		if !ok || !param.IsValid(v) {
			return false
		}
	}
	return true
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

// Replicates is a bitset of replicate numbers.
type Replicates uint64

// Completed reports whether n replicates have completed in
// the replicate set r.
func (r Replicates) Completed(n int) bool {
	mask := Replicates(1<<uint(n) - 1)
	return r&mask == mask
}

// Contains reports whether the replicate set r contains the
// replicate number rep.
func (r Replicates) Contains(rep int) bool {
	return r&(1<<uint(rep)) != 0
}

// Set sets the replicate number rep in the replicate set r.
func (r *Replicates) Set(rep int) {
	if rep >= 64 {
		panic(rep)
	}
	*r |= 1 << uint(rep)
}

// Clear clears the replicate number rep in the replicate set r.
func (r *Replicates) Clear(rep int) {
	if rep >= 64 {
		panic(rep)
	}
	*r &= ^(1 << uint(rep))
}

// Count returns the number of replicates defined in the
// replicate set r.
func (r Replicates) Count() int {
	return bits.OnesCount64(uint64(r))
}

// Next iterates over replicates. It returns the first replicate in
// the set as well as the replicate set with that replicate removed.
// -1 is returned if the replicate set is empty.
//
// Iteration can thus proceed:
//
//	var r Replicates
//	for num, r := r.Next(); num != -1; num, r = r.Next() {
//		// Process num
//	}
func (r Replicates) Next() (int, Replicates) {
	next := bits.TrailingZeros64(uint64(r))
	if next == 64 {
		return -1, r
	}
	r.Clear(next)
	return next, r
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

	// Replicates contains the set of completed replicates
	// comprising this trial. Replicates are stored in a bitset.
	Replicates Replicates

	// ReplicateMetrics breaks down metrics for each underlying replicate.
	// Valid only for trials that comprise multiple replicates.
	ReplicateMetrics map[int]Metrics

	// Runs stores the set of runs comprised by this trial.
	Runs []Run
}

// Timestamp returns the latest time at which any run comprising
// this trial was done.
func (t Trial) Timestamp() time.Time {
	var stamp time.Time
	for _, run := range t.Runs {
		if stamp.Before(run.Updated) {
			stamp = run.Updated
		}
	}
	return stamp
}

// Equal reports whether the two trials are equal.
func (t Trial) Equal(u Trial) bool {
	return t.Values.Equal(u.Values) && t.Metrics.Equal(u.Metrics)
}

// Range returns the range of the provided metric.
func (t Trial) Range(name string) (min, max float64) {
	first := true
	for _, metrics := range t.ReplicateMetrics {
		v, ok := metrics[name]
		if !ok {
			continue
		}
		if first {
			min, max, first = v, v, false
		} else if v < min {
			min = v
		} else if max < v {
			max = v
		}
	}
	if first {
		min, max = t.Metrics[name], t.Metrics[name]
	}
	return
}

// ReplicatedTrials constructs a single trial from the provided
// trials. The composite trial represents each replicate present in
// the provided replicates. Metrics are averaged. The provided trials
// cannot themselves contain multiple replicates.
func ReplicatedTrial(replicates []Trial) Trial {
	if len(replicates) == 0 {
		panic("no replicates provided")
	}
	// Select at most one "winning" for each replicate. If there are
	// multiple trials for a given replicate, we first try to pick a
	// non-pending one; if all trials are pending, we pick the latest
	// one.
	selected := make(map[int]Trial)
	for _, rep := range replicates {
		if rep.Replicates.Count() != 1 {
			log.Panicf("diviner.ReplicatedTrial: invalid trial %v with %d replicates", rep, rep.Replicates.Count())
		}
		n, _ := rep.Replicates.Next()
		prev, ok := selected[n]
		if !ok || prev.Pending && !rep.Pending || prev.Timestamp().Before(rep.Timestamp()) {
			selected[n] = rep
		}
	}

	var (
		counts = make(map[string]int)
		trial  = Trial{Values: replicates[0].Values, Metrics: make(Metrics), ReplicateMetrics: make(map[int]Metrics)}
	)
	for _, rep := range selected {
		for name, value := range rep.Metrics {
			counts[name]++
			n := float64(counts[name])
			trial.Metrics[name] = value/n + trial.Metrics[name]*(n-1)/n
		}
		for num, r := rep.Replicates.Next(); num != -1; num, r = r.Next() {
			trial.ReplicateMetrics[num] = rep.Metrics
		}
		trial.Pending = trial.Pending || rep.Pending
		trial.Replicates |= rep.Replicates
		trial.Runs = append(trial.Runs, rep.Runs...)
	}
	return trial
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
// objective, and an oracle responsible for generating trials.  Trials
// can either be managed by Diviner (through Run), or else handled
// natively within Go (through Acquire). Either Run or Acquire must
// be defined.
type Study struct {
	// Name is the name of the study.
	Name string
	// Objective is the objective to be maximized.
	Objective Objective
	// Params is the set of parameters accepted by this
	// study.
	Params Params

	// Replicates is the number of additional replicates required for
	// each trial in the study.
	Replicates int

	// Human-readable description of the study.
	Description string

	// Oracle is the oracle used to pick parameter values.
	Oracle Oracle `json:"-"` // TODO(marius): encode oracle name/type/params?

	// Run is called with a set of Values (i.e., a concrete
	// instantiation of values in the ranges as indicated by the black
	// box parameters defined above); it produces a run configuration
	// which is then used to conduct a trial of these parameter values.
	// The run's replicate number is passed in. (This may be used to,
	// e.g., select a model fold.) Parameter id is a unique id for the
	// run (vis-a-vis diviner's database). It may be used to name data
	// and other external resources associated with the run.
	Run func(vals Values, replicate int, id string) (RunConfig, error) `json:"-"`

	// Acquire returns the metrics associated with the set of
	// parameter values that are provided. It is used to support
	// (Go) native trials. Arguments are as in Run.
	Acquire func(vals Values, replicate int, id string) (Metrics, error) `json:"-"`
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
