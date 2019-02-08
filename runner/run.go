// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/log"
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
	w, err := runner.allocate(ctx)
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
	start := time.Now()
	out, err := w.Run(ctx, r.Config.Script)
	if err != nil {
		r.errorf("failed to start script: %s", err)
		return
	}
	r.setStatus(statusRunning, "")

	// Now that it's running, we tee the output of the script and
	// update its status. This allows scripts to, for example, output
	// progress bars or other things pertinent for monitoring. This
	// output is also teed to a local file for output.
	//
	// TODO(marius): make the local path configurable via the run.
	read, write := io.Pipe()
	writers := []io.Writer{write}
	f, err := os.Create(r.String())
	if err != nil {
		log.Error.Printf("failed to create log file for %s: %v", r, err)
	} else {
		writers = append(writers, f)
	}
	donec := make(chan struct{})
	go func() {
		scan := bufio.NewScanner(read)
		// In theory, Tensorflow's Progbar should display each update as a
		// new line when not outputting to a TTY, but its TTY detection
		// seems broken.
		scan.Split(scanProgress)
		for scan.Scan() {
			line := scan.Text()
			// TODO: make the prefix configurable, or perhaps even
			// different ways of communicating metrics.
			if strings.HasPrefix(line, "METRICS: ") {
				metrics, err := parseMetrics(strings.TrimPrefix(line, "METRICS: "))
				if err != nil {
					log.Error.Printf("error parsing metrics from run %s: %v", r, err)
				} else {
					r.report(metrics)
				}
			} else {
				r.setStatus(statusRunning, line)
			}
		}
		// (Ignore errors.)
		close(donec)
	}()
	_, err = io.Copy(io.MultiWriter(writers...), out)
	if e := out.Close(); err != nil && e != nil {
		err = e
	}
	if f != nil {
		if err := f.Close(); err != nil {
			log.Error.Printf("failed to close %s: %v", f.Name(), err)
		}
	}
	// Close the pipe and wait for the final status to be set so we don't
	// race for setting state.
	write.Close()
	select {
	case <-donec:
	case <-ctx.Done():
		r.error(ctx.Err())
		return
	}
	elapsed := time.Since(start)
	if err == nil {
		r.setStatus(statusOk, elapsed.String())
	} else {
		r.errorf("run failed after %s: %v", elapsed, err)
	}
}

// String returns a textual description of this run.
func (r *run) String() string {
	return fmt.Sprintf("study=%s,%s", r.Study.Name, r.Values)
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

// Status returns the run's current status and message.
func (r *run) Status() (status, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status, r.statusMessage
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
	if ni >= 0 && (ri < 0 || ni < ri) {
		return ni + 1, data[:ni], nil
	}
	if ri >= 0 {
		data = data[:ri]
		// Drop '\b's inserted by the progbar.
		for len(data) > 0 && data[len(data)-1] == '\b' {
			data = data[:len(data)-1]
		}
		return ri + 1, data, nil
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
