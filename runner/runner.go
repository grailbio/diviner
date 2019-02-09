// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package runner provides a simple parallel cluster runner for
// diviner studies. It uses bigmachine[1] to launch multiple machines
// over which trials are run in parallel, one trial per machine.
package runner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/divinerdb"
)

// IdleTime is the amount of time workers are allowed to remain idle
// before being stopped.
const idleTime = time.Minute

// A Runner is responsible for creating a cluster of machines and running
// trials on the cluster.
//
// Runner is also an http.Handler that prints trial statuses.
type Runner struct {
	study       diviner.Study
	db          *divinerdb.DB
	b           *bigmachine.B
	parallelism int

	requestc chan chan<- *worker

	// Time is the timestamp of runner.
	time time.Time

	// Idle maintains an idle list of workers, which may be reused
	// across invocations.
	idle []*worker

	mu       sync.Mutex
	counters map[string]int
	runs     []*run
	datasets map[string]*dataset

	nrun int
}

// New returns a new runner that will perform trials for the provided study,
// recording its results to the provided db, using the provided bigmachine.B
// to create a cluster of machines of at most hte size of the parallelism argument.
func New(study diviner.Study, db *divinerdb.DB, b *bigmachine.B, parallelism int) *Runner {
	return &Runner{
		study:       study,
		db:          db,
		b:           b,
		parallelism: parallelism,
		time:        time.Now(),
		counters:    make(map[string]int),
		requestc:    make(chan chan<- *worker),
		datasets:    make(map[string]*dataset),
	}
}

// StartTime returns the time that the runner was created.
func (r *Runner) StartTime() time.Time {
	return r.time
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
	io.WriteString(&tw, "id\t")
	io.WriteString(&tw, strings.Join(sorted, "\t"))
	fmt.Fprintln(&tw, "\tmetrics\tstatus")

	// TODO(marius): allow the user to pass in sort arguments, so that
	// the status page can serve as a scoreboard.
	row := make([]string, len(sorted)+4)
	r.mu.Lock()
	runs := r.runs
	r.mu.Unlock()
	for _, run := range runs {
		row[0] = fmt.Sprint(run.ID)
		for i, key := range sorted {
			if v, ok := run.Values[key]; ok {
				row[i+1] = v.String()
			} else {
				row[i+1] = "NA"
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

// Counters returns a set of runtime counters from this runner's Do loop.
func (r *Runner) Counters() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	counters := make(map[string]int)
	for k, v := range r.counters {
		counters[k] = v
	}
	return counters
}

// Do performs a single round of a study; at most ntrials. If
// ntrials is 0, then Do performs all trials returned by the study's
// oracle. Results are reported to the database as they are returned,
// and the runner's status page is updated continually with the
// latest available run metrics.
//
// Do does not perform retries for failed trials. Do should not be
// invoked concurrently.
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
		runs[i].ID = r.nrun
		r.nrun++
	}
	r.mu.Lock()
	r.runs = runs
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.runs = nil
		r.mu.Unlock()
	}()

	donec := make(chan *run)
	for _, run := range runs {
		run := run
		go func() {
			run.Do(ctx, r)
			select {
			case <-ctx.Done():
			case donec <- run:
			}
		}()
	}

	var (
		requests               []chan<- *worker
		workerc                = make(chan *worker)
		tick                   = time.NewTicker(10 * time.Second)
		nworker                = len(r.idle)
		ndone, nfail, nstarted int
	)
	r.mu.Lock()
	snapshot := make(map[string]int)
	for k, v := range r.counters {
		snapshot[k] = v
	}
	r.mu.Unlock()
	updateCounters := func() {
		r.mu.Lock()
		r.counters["nworker"] = nworker
		r.counters["ndone"] = snapshot["ndone"] + ndone
		r.counters["nfail"] = snapshot["nfail"] + nfail
		r.counters["nstarted"] = snapshot["nstarted"] + nstarted
		r.mu.Unlock()
	}
	send := func(c chan<- *worker, w *worker) {
		select {
		case <-ctx.Done():
		case c <- w:
		}
	}
	for ndone < len(runs) {
		updateCounters()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-tick.C:
			for len(r.idle) > 0 && time.Since(r.idle[0].IdleTime) > idleTime {
				var w *worker
				w, r.idle = r.idle[0], r.idle[1:]
				log.Printf("worker %s idled out from pool", w)
				w.Cancel()
				nworker--
			}
		case req := <-r.requestc:
			if len(r.idle) > 0 {
				var w *worker
				w, r.idle = r.idle[0], r.idle[1:]
				// Make sure that we attempt to return the worker on the correct
				// channel, e.g., if this idle worker was carried over from a previous
				// run.
				w.returnc = workerc
				go send(req, w)
				break
			}
			requests = append(requests, req)
			if r.parallelism > 0 && r.parallelism <= nworker {
				break
			}
			nworker++
			nstarted++
			go startWorker(ctx, r.b, workerc)
		case w := <-workerc:
			if err := w.Err(); err != nil {
				// TODO(marius): allocate a new worker to replace this one.
				nworker--
				log.Error.Printf("worker %s error: %v", w, err)
				break
			}
			if len(requests) > 0 {
				req := requests[0]
				requests = requests[1:]
				go send(req, w)
				break
			}
			// Otherwise we put it on a watch list. We don't reap the instance
			// right away because of the race between dataset completion and
			// runs starting.
			w.IdleTime = time.Now()
			r.idle = append(r.idle, w)
		case run := <-donec:
			s, m := run.Status()
			log.Printf("run %s: %s %s", run, s, m)
			ndone++
			switch status, message := run.Status(); status {
			case statusWaiting, statusRunning:
				log.Error.Printf("run %s returned with incomplete status %s", run, status)
			case statusOk:
				if err := r.db.Report(r.study, diviner.Trial{
					Values:  run.Values,
					Metrics: run.Metrics(),
				}); err != nil {
					log.Printf("error reporting metrics for run %s: %v", run, err)
					nfail++
				}
			case statusErr:
				nfail++
				log.Error.Printf("run %s error: %v", run, message)
			}
		}
	}
	updateCounters()
	return nfail == 0 && (ntrials == 0 || len(runs) < ntrials), nil
}

func (r *Runner) Cancel() {
	for _, w := range r.idle {
		w.Cancel()
	}
	r.idle = nil
}

// Allocate allocates a new worker and returns it. Workers must
// be returned after they are done by calling w.Return.
func (r *Runner) allocate(ctx context.Context) (*worker, error) {
	replyc := make(chan *worker)
	select {
	case r.requestc <- replyc:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case w := <-replyc:
		return w, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Dataset returns a named dataset as managed by this runner.
// If this is the first time the dataset is encountered, then the
// runner also begins dataset processing.
func (r *Runner) dataset(ctx context.Context, dataset diviner.Dataset) *dataset {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.datasets[dataset.Name]
	if ok {
		return d
	}
	d = newDataset(dataset)
	r.datasets[d.Name] = d
	go d.Do(ctx, r)
	return d
}
