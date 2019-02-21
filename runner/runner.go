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
	"github.com/grailbio/diviner/localdb"
)

// IdleTime is the amount of time workers are allowed to remain idle
// before being stopped.
const idleTime = time.Minute

// A Runner is responsible for creating a cluster of machines and running
// trials on the cluster.
//
// Runner is also an http.Handler that prints trial statuses.
type Runner struct {
	study diviner.Study
	db    diviner.Database

	requestc chan *request

	// Time is the timestamp of runner.
	time time.Time

	// Sessions stores the current set of bigmachine
	// sessions maintained by the runner, keyed by the
	// diviner system represented by the session.
	sessions map[*diviner.System]*session

	mu       sync.Mutex
	counters map[string]int
	runs     []*run
	datasets map[string]*dataset

	nrun int
}

// New returns a new runner that will perform trials for the provided study,
// recording its results to the provided database. The runner uses bigmachine
// to create new systems according to the run configurations returned from
// the study.
func New(study diviner.Study, db diviner.Database) *Runner {
	return &Runner{
		study:    study,
		db:       db,
		sessions: make(map[*diviner.System]*session),
		time:     time.Now(),
		counters: make(map[string]int),
		requestc: make(chan *request),
		datasets: make(map[string]*dataset),
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
		row[0] = fmt.Sprint(run.ID())
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
	complete, err := r.db.Runs(ctx, r.study, diviner.Complete)
	if err != nil && err != localdb.ErrNoSuchStudy { // XXX
		return false, err
	}
	trials := make([]diviner.Trial, len(complete))
	for i, run := range complete {
		var err error
		trials[i], err = run.Trial(ctx)
		if err != nil {
			return false, err
		}
	}
	log.Printf("requesting new points from oracle from %d trials", len(trials))
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
		runs[i].Run, err = r.db.New(ctx, r.study, values[i])
		if err != nil {
			return false, err
		}
		runs[i].Study = r.study
		runs[i].Values = values[i]
		runs[i].Config = r.study.Run(values[i])
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
		workerc                = make(chan *worker)
		tick                   = time.NewTicker(10 * time.Second)
		nworker                int
		ndone, nfail, nstarted int
	)
	for _, sess := range r.sessions {
		nworker += len(sess.Idle)
	}
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
	reply := func(r *request, w *worker) {
		select {
		case <-ctx.Done():
		case r.replyc <- w:
		}
	}
	for ndone < len(runs) {
		updateCounters()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-tick.C:
			for _, sess := range r.sessions {
				for len(sess.Idle) > 0 && time.Since(sess.Idle[0].IdleTime) > idleTime {
					var w *worker
					w, sess.Idle = sess.Idle[0], sess.Idle[1:]
					log.Printf("worker %s idled out from pool", w)
					w.Cancel()
					sess.N--
					nworker--
				}
			}
		case req := <-r.requestc:
			sess, ok := r.sessions[req.sys]
			if !ok {
				sess = &session{System: req.sys}
				var err error
				sess.B = bigmachine.Start(req.sys)
				if err != nil {
					return false, fmt.Errorf("failed to create session for system %s: %v", req.sys, err)
				}
				if sess.Name() != "testsystem" {
					sess.HandleDebugPrefix("/debug/"+sess.ID+"/", http.DefaultServeMux)
				}
				r.sessions[req.sys] = sess
			}
			if len(sess.Idle) > 0 {
				// Make sure that we attempt to return the worker on the correct
				// channel, e.g., if this idle worker was carried over from a previous
				// run.
				var w *worker
				w, sess.Idle = sess.Idle[0], sess.Idle[1:]
				w.returnc = workerc
				go reply(req, w)
				break
			}
			sess.Requests = append(sess.Requests, req)
			if sess.Parallelism > 0 && sess.Parallelism <= sess.N {
				break
			}
			sess.N++
			nworker++
			nstarted++
			w := &worker{
				Session: sess,
				returnc: workerc,
			}
			go w.Start(ctx)
		case w := <-workerc:
			sess := w.Session
			if err := w.Err(); err != nil {
				// TODO(marius): allocate a new worker to replace this one.
				sess.N--
				nworker--
				log.Error.Printf("worker %s error: %v", w, err)
				break
			}
			if len(sess.Requests) > 0 {
				var req *request
				req, sess.Requests = sess.Requests[0], sess.Requests[1:]
				go reply(req, w)
				break
			}
			// Otherwise we put it on a watch list. We don't reap the instance
			// right away because of the race between dataset completion and
			// runs starting.
			w.IdleTime = time.Now()
			sess.Idle = append(sess.Idle, w)
		case run := <-donec:
			s, m := run.Status()
			log.Printf("run %s: %s %s", run, s, m)
			ndone++
			switch status, message := run.Status(); status {
			case statusWaiting, statusRunning:
				log.Error.Printf("run %s returned with incomplete status %s", run, status)
			case statusOk:
				// TODO(marius): store failure state in the database, too.
				if err := run.Complete(ctx); err != nil {
					log.Error.Printf("failed to complete run %s: %v", run, err)
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
	for _, sess := range r.sessions {
		for _, w := range sess.Idle {
			w.Cancel()
		}
		sess.Idle = nil
	}
}

// Allocate allocates a new worker and returns it. Workers must
// be returned after they are done by calling w.Return.
func (r *Runner) allocate(ctx context.Context, sys *diviner.System) (*worker, error) {
	req := newRequest(sys)
	select {
	case r.requestc <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case w := <-req.Reply():
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

type request struct {
	replyc chan *worker
	sys    *diviner.System
}

func newRequest(sys *diviner.System) *request {
	return &request{make(chan *worker), sys}
}

func (r *request) Reply() <-chan *worker {
	return r.replyc
}

// Session stores the bigmachine session and associated state for a
// single system.
type session struct {
	// System is the diviner system from which this session is started.
	*diviner.System
	// B is the bigmachine session associated with this system.
	*bigmachine.B
	// Idle is a free pool of idle workers, in order of idle-ness.
	N int
	// Requests is the pending requests for workers in this system.
	Requests []*request
	// Idle is the set of idle workers in this system.
	Idle []*worker
}
