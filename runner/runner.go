// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package runner provides a simple parallel cluster runner for
// diviner studies. It uses bigmachine[1] to launch multiple machines
// over which trials are run in parallel, one trial per machine.
package runner

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/divinerdb"
	"golang.org/x/time/rate"
)

// A Runner is responsible for creating a cluster of machines and running
// trials on the cluster.
//
// Runner is also an http.Handler that prints trial statuses.
type Runner struct {
	study       diviner.Study
	db          *divinerdb.DB
	b           *bigmachine.B
	parallelism int

	mu   sync.Mutex
	runs []*run
}

// New returns a new runner that will perform trials for the provided study,
// recording its results to the provided db, using the provided bigmachine.B
// to create a cluster of machines of at most the size of the parallelism argument.
func New(study diviner.Study, db *divinerdb.DB, b *bigmachine.B, parallelism int) *Runner {
	return &Runner{study: study, db: db, b: b, parallelism: parallelism}
}

// ServeHTTP implements http.Handler, providing a simple status page used
// to examine the currently running trials.
func (r *Runner) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	sorted := make([]string, 0, len(r.study.Params))
	for key := range r.study.Params {
		sorted = append(sorted, key)
	}
	sort.Strings(sorted)
	var tw tabwriter.Writer
	tw.Init(w, 4, 4, 1, ' ', 0)
	io.WriteString(&tw, strings.Join(sorted, "\t"))
	fmt.Fprintln(&tw, "\tmetrics\tstatus")

	// Runs are just displayed in the order they were defined.
	row := make([]string, len(sorted)+3)
	r.mu.Lock()
	runs := r.runs
	r.mu.Unlock()
	for _, run := range runs {
		for i, key := range sorted {
			if v, ok := run.Values[key]; ok {
				row[i] = v.String()
			} else {
				row[i] = "NA"
			}
		}
		status, message := run.Status()
		if metrics := run.Metrics(); len(metrics) > 0 {
			keys := make([]string, 0, len(metrics))
			for key := range metrics {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			elems := make([]string, len(keys))
			for i, key := range keys {
				elems[i] = fmt.Sprintf("%s=%f", key, metrics[key])
			}
			row[len(row)-3] = strings.Join(elems, ",")
		} else {
			row[len(row)-3] = "NA"
		}

		row[len(row)-2] = status.String()
		row[len(row)-1] = message
		io.WriteString(&tw, strings.Join(row, "\t"))
		fmt.Fprintln(&tw)
	}
	tw.Flush()
}

// Do performs at most ntrials of a study. If ntrials is 0, then Do performs
// all trials returned by the study's oracle. Results are reported to the
// database as they are returned, and the runner's status page is updated
// continually with the latest available run metrics.
//
// Do does not perform retries for failed trials. Do cannot be invoked
// concurrently.
//
// BUG(marius): the runner should re-create failed machines.
func (r *Runner) Do(ctx context.Context, ntrials int) (done bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	trials, err := r.db.Load(r.study)
	if err != nil {
		return false, err
	}
	values, err := r.study.Oracle.Next(trials, r.study.Params, r.study.Objective, ntrials)
	if err != nil {
		return false, err
	}
	if len(values) == 0 {
		return true, nil
	}
	runs := make([]*run, len(values))
	for i := range values {
		runs[i] = new(run)
		runs[i].Study = r.study
		runs[i].Values = values[i]
		runs[i].Config = r.study.Run(values[i])
	}
	r.mu.Lock()
	r.runs = runs
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.runs = nil
		r.mu.Unlock()
	}()
	var (
		todoc       = make(chan *run, len(runs))
		donec       = make(chan *run)
		doneworkerc = make(chan struct{})
	)
	for _, run := range runs {
		todoc <- run
	}
	close(todoc)
	log.Printf("starting %d runs", len(runs))
	nworker := r.parallelism
	if nworker == 0 || nworker > len(runs) {
		nworker = len(runs)
	}
	for i := 0; i < nworker; i++ {
		go func() {
			err := worker(ctx, r.b, r.study, todoc, donec)
			if err != nil {
				log.Error.Printf("worker failure: %v", err)
			}
			doneworkerc <- struct{}{}
		}()
	}

	var (
		workersVar = expvar.NewInt("workers")
		doneVar    = expvar.NewInt("done")
		failedVar  = expvar.NewInt("failed")
	)
	workersVar.Set(int64(len(runs)))
	expvar.Publish("queued", expvar.Func(func() interface{} {
		return len(todoc)
	}))
	expvar.Publish("processing", expvar.Func(func() interface{} {
		return len(runs) - int(doneVar.Value()) - int(failedVar.Value()) - len(todoc)
	}))

	var needmore bool
runloop:
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case run := <-donec:
			status, message := run.Status()
			if message != "" {
				log.Printf("run %s: %s %s", run, status, message)
			} else {
				log.Printf("run %s: %s", run, status)
			}
			switch status {
			case statusWaiting, statusRunning:
				panic(status)
			case statusOk:
				metrics := run.Metrics()
				if len(metrics) == 0 {
					// TODO: also verify that all metrics have been reported
					log.Error.Printf("run %s reported no metrics", run)
					needmore = false
				}
				if err := r.db.Report(r.study, diviner.Trial{
					Values:  run.Values,
					Metrics: metrics,
				}); err != nil {
					log.Printf("error reporting metrics for run %s: %v", run, err)
					needmore = false
				}
				doneVar.Add(1)
			case statusErr:
				failedVar.Add(1)
			}
		case <-doneworkerc:
			workersVar.Add(-1)
			if workersVar.Value() == 0 {
				break runloop
			}
		}
	}
	log.Printf("runs complete: total=%d done=%d failed=%d", len(runs), doneVar.Value(), failedVar.Value())
	return !needmore && (ntrials == 0 || len(runs) < ntrials) && len(todoc) == 0, nil
}

var (
	machineRetry = retry.Jitter(retry.Backoff(30*time.Second, 5*time.Minute, 1.5), 0.5)
	machineLimit = rate.NewLimiter(rate.Limit(0.5), 3)
	allocating   = expvar.NewInt("allocating")
	allocated    = expvar.NewInt("allocated")
)

// Worker starts a worker machine, processing runs as requested by
// the main loop. Worker returns when the todo channel is closed or
// if the machine fails.
func worker(ctx context.Context, b *bigmachine.B, study diviner.Study, enterc <-chan *run, returnc chan<- *run) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	var m *bigmachine.Machine
	// First try to allocate a machine. We keep a jittered retry schedule so that
	// we have a chance to get through the various rate limits.
	allocating.Add(1)
	defer func() {
		if m == nil {
			allocating.Add(-1)
		}
	}()
	for try := 0; ; try++ {
		// TODO(marius): distinguish between true allocation errors
		// and others that may occur.
		if err := machineLimit.Wait(ctx); err != nil {
			return err
		}
		machines, err := b.Start(ctx, 1, bigmachine.Services{
			"Cmd": &commandService{},
		})
		if err == nil && len(machines) == 0 {
			err = errors.New("no machines allocated")
		} else if err == nil {
			m = machines[0]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Minute):
				err = errors.New("timeout while waiting for machine to start")
			case <-m.Wait(bigmachine.Running):
				if m.State() != bigmachine.Running {
					err = fmt.Errorf("machine failed to start: %v", m.Err())
				}
			}
		}
		if err == nil {
			break
		}
		// HACK: this should be propagated as a semantic error annotation.
		if !strings.Contains(err.Error(), "InstanceLimitExceeded") {
			log.Error.Printf("failed to allocate machine: %v", err)
		}
		if err := retry.Wait(ctx, machineRetry, try); err != nil {
			return err
		}
	}
	defer m.Cancel()
	allocating.Add(-1)
	allocated.Add(1)
	log.Printf("allocated machine %s in %s", m.Addr, time.Since(start))

runloop:
	// Process trials.
	for run := range enterc {
		start := time.Now()

		// For each run, we reset the "scratch" space used to store command
		// input and output, upload local files, and then run the actual
		// command, while monitoring its output.
		if err := m.Call(ctx, "Cmd.Reset", struct{}{}, nil); err != nil {
			run.Errorf("failed to reset scratch space: %v", err)
			returnc <- run
			continue
		}

		for _, path := range run.Config.LocalFiles {
			file := fileLiteral{Name: path}
			var err error
			file.Contents, err = ioutil.ReadFile(path)
			if err != nil {
				run.Errorf("failed to read local file %s: %v", path, err)
				returnc <- run
				continue runloop
			}
			if err := m.Call(ctx, "Cmd.WriteFile", file, nil); err != nil {
				run.Errorf("failed to upload file %s: %v", path, err)
				returnc <- run
				continue runloop
			}
		}

		var out io.ReadCloser
		// TODO(marius): allow bigmachine to run under the default user.
		script := `set -ex; su - ubuntu; export HOME=/home/ubuntu; ` + run.Config.Script
		if err := m.Call(ctx, "Cmd.Run", []string{"bash", "-c", script}, &out); err != nil {
			run.Errorf("failed to run script: %v", err)
			returnc <- run
			continue runloop
		}

		run.SetStatus(statusRunning, "")

		// Now that it's running, we tee the output of the script and
		// update its status. This allows scripts to, for example, output
		// progress bars or other things pertinent for monitoring. This
		// output is also teed to a local file for output.
		//
		// TODO(marius): make the local path configurable via the run.
		r, w := io.Pipe()
		writers := []io.Writer{w}
		f, err := os.Create(run.String())
		if err != nil {
			log.Error.Printf("failed to create log file for %s: %v", run, err)
		} else {
			writers = append(writers, f)
		}
		donec := make(chan struct{})
		go func() {
			scan := bufio.NewScanner(r)
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
						log.Error.Printf("error parsing metrics from run %s: %v", run, err)
					} else {
						run.Report(metrics)
					}
				} else {
					run.SetStatus(statusRunning, line)
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
		w.Close()
		select {
		case <-donec:
		case <-ctx.Done():
			return ctx.Err()
		}
		elapsed := time.Since(start)
		if err == nil {
			run.SetStatus(statusOk, elapsed.String())
		} else {
			run.Errorf("run failed after %s: %v", elapsed, err)
		}
		returnc <- run
	}
	return nil
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
