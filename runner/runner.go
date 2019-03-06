// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package runner provides a simple parallel cluster runner for
// diviner studies. It uses bigmachine[1] to launch multiple machines
// over which trials are run in parallel, one trial per machine.
package runner

import (
	"bytes"
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
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/diviner"
	"github.com/grailbio/diviner/localdb"
)

// IdleTime is the amount of time workers are allowed to remain idle
// before being stopped.
const idleTime = 5 * time.Minute

// A Runner is responsible for creating a cluster of machines and running
// trials on the cluster.
//
// Runner is also an http.Handler that prints trial statuses.
type Runner struct {
	db diviner.Database

	requestc chan *request

	// Time is the timestamp of runner.
	time time.Time

	ctx    context.Context
	cancel func()

	mu       sync.Mutex
	counters map[string]int
	// Runs maps study names to the list of runs for this study.
	runs     map[string][]*run
	datasets map[string]*dataset

	nrun int
}

// New returns a new runner that will perform trials, recording its
// results to the provided database. The runner uses bigmachine to
// create new systems according to the run configurations returned
// from the study. The caller must start the runner's run loop by
// calling Do.
func New(db diviner.Database) *Runner {
	return &Runner{
		db:       db,
		time:     time.Now(),
		counters: make(map[string]int),
		requestc: make(chan *request),
		datasets: make(map[string]*dataset),
		runs:     make(map[string][]*run),
	}
}

// StartTime returns the time that the runner was created.
func (r *Runner) StartTime() time.Time {
	return r.time
}

// ServeHTTP implements http.Handler, providing a simple status page used
// to examine the currently running trials, organized by study.
func (r *Runner) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var buf bytes.Buffer // so we don't hold the lock while waiting for clients
	r.mu.Lock()
	names := make([]string, 0, len(r.runs))
	for name := range r.runs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if len(r.runs[name]) == 0 {
			continue
		}
		study := r.runs[name][0].Study

		var tw tabwriter.Writer
		tw.Init(&buf, 4, 4, 1, ' ', 0)

		fmt.Fprintf(&tw, "study %s:\n", study.Name)
		fmt.Fprintln(&tw, "\tparams:")
		for _, param := range study.Params.Sorted() {
			fmt.Fprintf(&tw, "\t\t%s:\t%s\n", param.Name, param)
		}

		fmt.Fprint(&tw, "\ttrials:\t")
		sorted := make([]string, 0, len(study.Params))
		for key := range study.Params {
			sorted = append(sorted, key)
		}
		sort.Strings(sorted)
		io.WriteString(&tw, "id\t")
		io.WriteString(&tw, strings.Join(sorted, "\t"))
		fmt.Fprintln(&tw, "\truntime\tmetrics\tstatus")

		// TODO(marius): allow the user to pass in sort arguments, so that
		// the status page can serve as a scoreboard.
		row := make([]string, len(sorted)+5)

		for _, run := range r.runs[name] {
			row[0] = fmt.Sprint(run.ID())
			for i, key := range sorted {
				if v, ok := run.Values[key]; ok {
					row[i+1] = v.String()
				} else {
					row[i+1] = "NA"
				}
			}
			status, message, elapsed := run.Status()
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
				row[len(row)-4] = strings.Join(elems, ",")
			} else {
				row[len(row)-4] = "NA"
			}

			row[len(row)-3] = elapsed.String()
			row[len(row)-2] = status.String()
			row[len(row)-1] = message
			io.WriteString(&tw, "\t\t\t")
			io.WriteString(&tw, strings.Join(row, "\t"))
			fmt.Fprintln(&tw)
		}
		tw.Flush()
	}
	r.mu.Unlock()
	_, _ = io.Copy(w, &buf)
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

// Loop is the runner's main run loop, managing clusters of machines
// and allocating workers among the runs. The runner stops doing work
// when the provided context is canceled. All errors are fatal: the
// runner may not be revived.
//
// BUG(marius): the runner should re-create failed machines.
func (r *Runner) Loop(ctx context.Context) error {
	var (
		tick                   = time.NewTicker(10 * time.Second)
		nworker                int
		ndone, nfail, nstarted int
		workerc                = make(chan *worker)

		// Sessions stores the current set of bigmachine
		// sessions maintained by the runner, keyed by the
		// diviner system represented by the session.
		sessions = make(map[*diviner.System]*session)
	)
	defer func() {
		for _, sess := range sessions {
			for _, w := range sess.Idle {
				w.Cancel()
			}
			sess.Idle = nil
		}
	}()
	updateCounters := func() {
		r.mu.Lock()
		r.counters["nworker"] = nworker
		r.counters["ndone"] = ndone
		r.counters["nfail"] = nfail
		r.counters["nstarted"] = nstarted
		r.mu.Unlock()
	}
	reply := func(r *request, w *worker) {
		select {
		case <-ctx.Done():
		case r.replyc <- w:
		}
	}
	for {
		updateCounters()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			for _, sess := range sessions {
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
			sess, ok := sessions[req.sys]
			if !ok {
				sess = &session{System: req.sys}
				var err error
				sess.B = bigmachine.Start(req.sys)
				if err != nil {
					return fmt.Errorf("failed to create session for system %s: %v", req.sys, err)
				}
				if sess.Name() != "testsystem" {
					sess.HandleDebugPrefix("/debug/"+sess.ID+"/", http.DefaultServeMux)
				}
				sessions[req.sys] = sess
			}
			if len(sess.Idle) > 0 {
				var w *worker
				w, sess.Idle = sess.Idle[0], sess.Idle[1:]
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
		}
	}
	updateCounters()
	return nil
}

// Run performs a single run with the provided study and values. The
// run is registered in the runner's configured database, and its
// status is maintained throughout the course of execution. Run
// returns when the run is complete (its status may be inspected by
// methods on diviner.Run); all errors are runtime errors, not errors
// of the run itself. The run is registered with the runner and will
// show up in the various introspection facilities. Run
func (r *Runner) Run(ctx context.Context, study diviner.Study, values diviner.Values) (diviner.Run, error) {
	config, err := study.Run(values)
	if err != nil {
		return nil, err
	}
	run := new(run)
	run.Run, err = r.db.New(ctx, study, values, config)
	if err != nil {
		return nil, err
	}
	run.Study = study
	run.Values = values
	run.Config = config
	r.add(run)
	defer r.remove(run)
	run.Do(ctx, r)
	status, message, elapsed := run.Status()
	log.Printf("run %s: %s %s %s", run, status, message, elapsed)
	state := diviner.Failure
	switch status {
	case statusWaiting, statusRunning:
		log.Error.Printf("run %s returned with incomplete status %s", run, status)
	case statusOk:
		state = diviner.Success
	case statusErr:
		log.Error.Printf("run %s error: %v", run, message)
	}
	if err := run.Complete(ctx, state, elapsed); err != nil {
		log.Error.Printf("failed to complete run %s: %v", run, err)
		return nil, err
	}
	return run.Run, nil
}

func (r *Runner) Round(ctx context.Context, study diviner.Study, ntrials int) (done bool, err error) {
	complete, err := r.db.Runs(ctx, study.Name, diviner.Success)
	if err != nil && err != localdb.ErrNoSuchStudy {
		return false, err
	}
	trials := make([]diviner.Trial, len(complete))
	for i, run := range complete {
		trials[i].Values = run.Values()
		var err error
		trials[i].Metrics, err = run.Metrics(ctx)
		if err != nil {
			return false, err
		}
	}
	log.Printf("%s: requesting new points from oracle from %d trials", study.Name, len(trials))
	values, err := study.Oracle.Next(trials, study.Params, study.Objective, ntrials)
	if err != nil {
		return false, err
	}
	if len(values) == 0 {
		return true, nil
	}
	runs := make([]diviner.Run, len(values))
	err = traverse.Each(len(values), func(i int) (err error) {
		runs[i], err = r.Run(ctx, study, values[i])
		return
	})
	if err != nil {
		return false, err
	}
	for _, run := range runs {
		if run.State() != diviner.Success {
			return false, nil
		}
	}
	return ntrials == 0 || (len(runs) < ntrials), nil
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

func (r *Runner) add(run *run) {
	r.mu.Lock()
	r.runs[run.Study.Name] = append(r.runs[run.Study.Name], run)
	r.mu.Unlock()
}

func (r *Runner) remove(run *run) {
	r.mu.Lock()
	defer r.mu.Unlock()
	runs := r.runs[run.Study.Name]
	for i := range runs {
		if runs[i] == run {
			runs[i] = runs[len(runs)-1]
			runs = runs[:len(runs)-1]
			r.runs[run.Study.Name] = runs
			return
		}
	}
	panic("run not found")
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
