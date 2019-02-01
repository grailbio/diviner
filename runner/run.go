// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"fmt"
	"sync"

	"github.com/grailbio/diviner"
)

// Status describes the current status of a run.
type status int

const (
	// StatusWaiting indicates that a run has not yet been scheduled.
	statusWaiting status = iota
	// StatusRunning indicates that a run is currently being executed
	// on a worker machine.
	statusRunning
	// StatusOk indicates that the run completed successfully.
	statusOk
	// StatusErr indicates that the run failed.
	statusErr
)

// String returns a simple string describing the status s.
func (s status) String() string {
	switch s {
	case statusWaiting:
		return "waiting"
	case statusRunning:
		return "running"
	case statusOk:
		return "ok"
	case statusErr:
		return "error"
	default:
		panic(s)
	}
}

// A run represents a single run. It contains everything required to perform
// the run on a worker machine. Instances of run are used by the runner to
// coordinate runs of individual trials.
type run struct {
	// Params is the set of values for the trial represented by this run.
	Values diviner.Values

	// Study is the study associated with this trial.
	Study diviner.Study

	// Config is the run config for this trial.
	Config diviner.RunConfig

	mu            sync.Mutex
	status        status
	statusMessage string
	// Metrics stores the last reported metrics for the run.
	metrics diviner.Metrics
}

// String returns a textual description of this run.
func (r *run) String() string {
	return fmt.Sprintf("study=%s,%s", r.Study.Name, r.Values)
}

// Report updates the run's metrics to those provided.
func (r *run) Report(metrics diviner.Metrics) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = metrics
}

// Metrics returns the last reported metrics for this run.
func (r *run) Metrics() diviner.Metrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.metrics) == 0 {
		return nil
	}
	copy := make(diviner.Metrics, len(r.metrics))
	for k, v := range r.metrics {
		copy[k] = v
	}
	return copy
}

// SetStatus sets the status for the run.
func (r *run) SetStatus(status status, message string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = status
	r.statusMessage = message
}

// Errorf sets the run's status to statusErr and formats the
// status string using fmt.Sprintf.
func (r *run) Errorf(format string, v ...interface{}) {
	r.SetStatus(statusErr, fmt.Sprintf(format, v...))
}

// Status returns the run's current status and message.
func (r *run) Status() (status, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status, r.statusMessage
}
