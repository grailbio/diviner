// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/diviner"
)

var metricsPrefix = []byte("METRICS: ")

const timeLayout = "20060102.150405"

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

// Done tells whether the status indicatest that the process
// has completed.
func (s status) Done() bool {
	return s == statusOk || s == statusErr
}

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
	diviner.Run

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
	// Time when the run first entered running state.
	start time.Time
}

// Do performs the run using the provided runner after first coordinating
// that its dataset dependencies are satisfied through the same.
func (r *run) Do(ctx context.Context, runner *Runner) {
	// First, make sure that our dependent datasets have completed
	// without error.
	datasets := make([]*dataset, len(r.Config.Datasets))
	for i, dataset := range r.Config.Datasets {
		// This will kick off processing if it's not already started.
		datasets[i] = runner.dataset(ctx, dataset)
	}
	if len(datasets) > 0 {
		r.setStatus(statusWaiting, "waiting for datasets to complete processing")
	}
	for _, dataset := range datasets {
		select {
		case <-ctx.Done():
			r.error(ctx.Err())
			return
		case <-dataset.Done():
			if err := dataset.Err(); err != nil {
				r.errorf("failed to process dataset %s: %v", dataset.Name, err)
				return
			}
		}
	}
	r.setStatus(statusWaiting, "waiting for worker")
	w, err := runner.allocate(ctx, r.Config.System)
	if err != nil {
		r.error(err)
		return
	}
	defer w.Return()

	r.setStatus(statusRunning, "")
	if err := w.Reset(ctx); err != nil {
		r.error(err)
		return
	}
	if err := w.CopyFiles(ctx, r.Config.LocalFiles); err != nil {
		r.error(err)
		return
	}
	r.mu.Lock()
	r.start = time.Now()
	r.mu.Unlock()
	out, err := w.Run(ctx, r.Config.Script)
	if err != nil {
		r.errorf("failed to start script: %s", err)
		return
	}
	r.setStatus(statusRunning, "")

	scan := bufio.NewScanner(out)
	// ScanProgress tells us how to scan "progress bar" output from
	// the likes of Tensorflow. This allows us to properly separate these
	// out as lines for status output and also filter them when persisting
	// run logs.
	scan.Split(scanProgress)
	for scan.Scan() {
		line := scan.Bytes()
		// TODO: make the prefix configurable, or perhaps even
		// different ways of communicating metrics.
		if bytes.HasPrefix(line, metricsPrefix) {
			line := string(line)
			metrics, err := parseMetrics(strings.TrimPrefix(line, "METRICS: "))
			if err != nil {
				log.Error.Printf("error parsing metrics from run %s: %v", r, err)
			} else {
				r.report(metrics)
				if err := r.Run.Update(ctx, metrics); err != nil {
					log.Error.Printf("failed to report metrics to DB: %v", err)
				}
			}
		} else {
			progress := len(line) > 0 && line[len(line)-1] == '\r'
			if progress {
				// Drop any '\b's inserted by the progbar.
				for len(line) > 0 && (line[len(line)-1] == '\r' || line[len(line)-1] == '\b') {
					line = line[:len(line)-1]
				}
			}

			r.setStatus(statusRunning, string(line))
			if !progress {
				if _, err := r.Run.Write(line); err != nil {
					log.Error.Printf("%s: write: %v", r, err)
				}
				if _, err := r.Run.Write([]byte{'\n'}); err != nil {
					log.Error.Printf("%s: write %v", r, err)
				}
			}
		}
		// We have to wait for some output in order to respond to context
		// cancellations.
		if err := ctx.Err(); err != nil {
			break
		}
	}
	if err := r.Run.Flush(); err != nil {
		log.Error.Printf("%s: flush: %v", r, err)
	}

	elapsed := time.Since(r.start)
	if err := scan.Err(); err == nil {
		r.setStatus(statusOk, elapsed.String())
	} else {
		r.errorf("run failed after %s: %v", elapsed, err)
	}
}

// String returns a textual description of this run.
func (r *run) String() string {
	return r.ID()
}

// Report merges the provided metrics into the current run metrics.
func (r *run) report(metrics diviner.Metrics) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics.Merge(metrics)
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
func (r *run) setStatus(status status, message string) {
	if err := r.SetStatus(context.Background(), fmt.Sprintf("%s: %s", status, message)); err != nil {
		log.Error.Printf("run %s: error setting status: %v", r, message)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = status
	r.statusMessage = message
}

// Errorf sets the run's status to statusErr and formats the
// status string using fmt.Sprintf.
func (r *run) errorf(format string, v ...interface{}) {
	r.setStatus(statusErr, fmt.Sprintf(format, v...))
}

// Error sets the run's status to statusErr, formatting the
// arguments in the manner of fmt.Sprint.
func (r *run) error(v ...interface{}) {
	r.setStatus(statusErr, fmt.Sprint(v...))
}

// Status returns the run's current status and message, and elapsed runtime.
func (r *run) Status() (status, string, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var elapsed time.Duration
	if !r.start.IsZero() {
		elapsed = time.Since(r.start)
	}
	return r.status, r.statusMessage, elapsed
}

// ScanProgress scans lines of output from a trial script, anticipating
// "progress bar" style output, like that used in Tensorflow's progbar.
func scanProgress(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	var (
		ni = bytes.IndexByte(data, '\n')
		ri = bytes.IndexByte(data, '\r')
	)
	if ni >= 0 {
		if ri < 0 || ni < ri {
			return ni + 1, data[:ni], nil
		}
		if ri == ni-1 { // "\r\n"
			return ni + 1, data[:ni-1], nil
		}
	}
	if ri >= 0 {
		return ri + 1, data[:ri+1], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	// Need more data.
	return 0, nil, nil
}

func parseMetrics(line string) (diviner.Metrics, error) {
	elems := strings.Split(line, ",")
	metrics := make(diviner.Metrics)
	for _, elem := range elems {
		if elem == "" {
			continue
		}
		parts := strings.SplitN(elem, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("bad metric %s", elem)
		}
		var err error
		metrics[parts[0]], err = strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing metric %s: %v", parts[1], err)
		}
	}
	return metrics, nil
}
