// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner

import (
	"context"
	"io"
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

// A Run is a single run, which may either be ongoing (state pending) or
// complete (in which case a Trial may be derived from it).
type Run interface {
	// Writing to a run is taken as an informational log message.
	// Logs are stored persistently and may be retrieved later.
	// It is invalid to write to a completed run.
	io.Writer
	// Flush ensures that all writes to this run has been persisted.
	Flush() error
	// ID returns a unique identifier for this run, which may be used
	// to look the run up in a database later.
	ID() string
	// State reports the current run state.
	State() RunState
	// Update updates the current metrics for the run. The
	// last metric before the run has completed is taken as the
	// run's final output.
	Update(ctx context.Context, metrics Metrics) error
	// SetStatus sets the run's current status, used for informational
	// and diagnostic purposes.
	SetStatus(ctx context.Context, message string) error
	// Status
	Status(ctx context.Context) (string, error)
	// Values returns the values that are computed by this run.
	Values() Values
	// Metrics returns the most recent metrics for this run. If the
	// run's state is Success, then these metrics are the final trial
	// metrics. Empty metrics are returned if no metrics have been
	// reported.
	Metrics(ctx context.Context) (Metrics, error)
	// Complete is called when the run has completed and no more
	// metrics or logs will be reported. The run is marked as the given state
	// (which cannot be Pending).
	Complete(ctx context.Context, state RunState) error
	// Log returns a reader from which the run's log messages may
	// be read from persistent storage. If follow is true, the returned
	// reader streams ongoing log updates indefinitely.
	Log(follow bool) io.Reader
}

// A Database is used to track studies and their results.
type Database interface {
	// New creates a new run in pending state. The caller can then
	// update the run's metrics and complete it once it has finished.
	New(ctx context.Context, study Study, values Values) (Run, error)
	// Runs returns all runs in the study with the provided run states.
	Runs(ctx context.Context, study Study, states RunState) ([]Run, error)
	// Run looks up a single run by its run ID.
	Run(ctx context.Context, id string) (Run, error)
}
