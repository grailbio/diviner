// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner

import (
	"context"
	"errors"
	"io"
	"time"
)

// RunState describes the current state of a particular run.
type RunState int

const (
	// Pending indicates that the run has not yet completed.
	Pending RunState = 1 << iota
	// Success indicates that the run has completed and represents
	// a successful trial.
	Success
	// Failure indicates that the run failed.
	Failure

	// Any contains all run states.
	Any = Pending | Success | Failure
)

// String returns a simple textual representation of a run state.
func (s RunState) String() string {
	switch s {
	case 0:
		return "unknown"
	case Pending:
		return "pending"
	case Success:
		return "success"
	case Failure:
		return "failure"
	default:
		return "INVALID"
	}
}

// A Run describes a single run, which, upon successful completion,
// represents a Trial. Runs are managed by a Database.
type Run struct {
	// Values is the set of parameter values represented by this run.
	Values

	// Study is the name of the study serviced by this run.
	Study string
	// Seq is a sequence number assigned to each run in a study.
	// Together, the study and sequence number uniquely names
	// a run.
	Seq uint64

	// Replicate is the replicate of this run.
	Replicate int

	// State is the current state of the run. See RunState for
	// descriptions of these.
	State RunState
	// Status is a human-consumable status indicating the status
	// of the run.
	Status string

	// Config is the RunConfig for this run.
	Config RunConfig

	// Created is the time at which the run was created.
	Created time.Time
	// Updated is the last time the run's state was updated. Updated is
	// used as a keepalive mechanism.
	Updated time.Time
	// Runtime is the runtime duration of the run.
	Runtime time.Duration

	// Number of times the run was retried.
	Retries int

	// Metrics is the history of metrics, in the order reported by the
	// run.
	//
	// TODO(marius): include timestamps for these, or some other
	// reference (e.g., runtime).
	Metrics []Metrics
}

// Trial returns the Trial represented by this run.
//
// TODO(marius): allow other metric selection policies
// (e.g., minimize train and test loss difference)
func (r Run) Trial() Trial {
	trial := Trial{Values: r.Values, Pending: r.State != Success, Runs: []Run{r}}
	trial.Replicates.Set(r.Replicate)
	if len(r.Metrics) > 0 {
		trial.Metrics = r.Metrics[len(r.Metrics)-1]
	}
	return trial
}

// ErrNotExist is returned from a database when a study or run does not exist.
var ErrNotExist = errors.New("study or run does not exist")

// A Database is used to track and manage studies and runs.
type Database interface {
	// CreateStudyIfNotExist creates a new study from the provided Study value.
	// If the study already exists, this is a no-op.
	CreateStudyIfNotExist(ctx context.Context, study Study) (created bool, err error)
	// LookupStudy returns the study with the provided name.
	LookupStudy(ctx context.Context, name string) (Study, error)
	// ListStudies returns the set of studies matching the provided prefix and whose
	// last update time is not before the provided time.
	ListStudies(ctx context.Context, prefix string, since time.Time) ([]Study, error)

	// NextSeq reserves and returns the next run sequence number for the
	// provided study.
	NextSeq(ctx context.Context, study string) (uint64, error)
	// InsertRun inserts the provided run into a study. The run's study,
	// values, and config must be populated; other fields are ignored.
	// If the sequence number is provided (>0), then it is assumed to
	// have been reserved by NextSeq. The run's study must already
	// exist, and the returned Run is assigned a sequence number, state,
	// and creation time.
	InsertRun(ctx context.Context, run Run) (Run, error)
	// UpdateRun updates the run named by the provided study and
	// sequence number with the given run state, message, runtime, and
	// current retry sequence.
	// UpdateRun is used also as a keepalive mechanism: runners must
	// call UpdateRun frequently in order to have the run considered
	// live by Diviner's tooling.
	UpdateRun(ctx context.Context, study string, seq uint64, state RunState, message string, runtime time.Duration, retry int) error
	// AppendRunMetrics reports a new set of metrics to the run named by the provided
	// study and sequence number.
	AppendRunMetrics(ctx context.Context, study string, seq uint64, metrics Metrics) error

	// ListRuns returns the set of runs in the provided study matching the queried
	// run states. ListRuns only returns runs that have been updated since the provided
	// time.
	ListRuns(ctx context.Context, study string, states RunState, since time.Time) ([]Run, error)
	// LookupRun returns the run named by the provided study and sequence number.
	LookupRun(ctx context.Context, study string, seq uint64) (Run, error)

	// Log obtains a reader for the logs emitted by the run named by the
	// study and sequence number. If follow is true, the returned reader
	// is a perpetual stream, updated as new log entries are appended.
	Log(study string, seq uint64, follow bool) io.Reader

	// Logger returns an io.WriteCloser, to which log messages can be written,
	// for the run named by a study and sequence number.
	Logger(study string, seq uint64) io.WriteCloser
}

// Trials queries the database db for all runs in the provided study,
// and returns a set of composite trials for each replicate of a
// value set. The returned map maps value sets to these composite
// trials.
//
// Trial metrics are averaged across successful and pending runs;
// flags are set on the returned trials to indicate which replicates
// they comprise and whether any pending results were used.
//
// TODO(marius): this is a reasonable approach for some metrics, but
// not for others. We should provide a way for users to (e.g., as
// part of a study definition) to define their own means of defining
// composite metrics, e.g., by intepreting metrics from each run, or
// their outputs directly (e.g., predictions from an evaluation run).
func Trials(ctx context.Context, db Database, study Study) (*Map, error) {
	runs, err := db.ListRuns(ctx, study.Name, Success|Pending, time.Time{})
	if err != nil && err != ErrNotExist {
		return nil, err
	}
	replicates := NewMap()
	for i := range runs {
		var trials []Trial
		if v, ok := replicates.Get(runs[i].Values); ok {
			trials = v.([]Trial)
		}
		trials = append(trials, runs[i].Trial())
		replicates.Put(runs[i].Values, trials)
	}
	trials := NewMap()
	replicates.Range(func(key Value, v interface{}) {
		var (
			reps   = v.([]Trial)
			counts = make(map[string]int)
			values = key.(Values)
			trial  = Trial{Values: values, Metrics: make(Metrics)}
		)
		for _, rep := range reps {
			if trial.Replicates&rep.Replicates != 0 {
				// TODO(marius): pick "best" replicate?
				continue
			}
			for name, value := range rep.Metrics {
				counts[name]++
				n := float64(counts[name])
				trial.Metrics[name] = value/n + trial.Metrics[name]*(n-1)/n
			}
			trial.Pending = trial.Pending || rep.Pending
			trial.Replicates |= rep.Replicates
			trial.Runs = append(trial.Runs, rep.Runs...)
		}
		trials.Put(&values, trial)
	})
	return trials, nil
}
