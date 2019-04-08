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
)

const (
	// IdleTime is the amount of time workers are allowed to remain idle
	// before being stopped.
	idleTime = 5 * time.Minute

	keepaliveInterval = 15 * time.Second
)

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
			row[0] = fmt.Sprint(run.Run.Seq)
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
outer:
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
					if w.Session != sess {
						panic(w)
					}
					w.Cancel()
					sess.release()
					nworker--
				}
			}
		case req := <-r.requestc:
			if len(req.sessions) > 0 {
				panic(req)
			}
			var reqSessions []*session
			for _, sys := range req.sys {
				sess, ok := sessions[sys]
				if !ok {
					sess = &session{
						System:   sys,
						B:        bigmachine.Start(sys),
						Requests: map[*request]struct{}{},
					}
					if sess.System.Name() != "testsystem" {
						sess.B.HandleDebugPrefix(fmt.Sprintf("/debug/%s/", sess.System.ID), http.DefaultServeMux)
					}
					sessions[sys] = sess
				}
				req.attach(sess)
				if len(sess.Idle) > 0 {
					var w *worker
					w, sess.Idle = sess.Idle[0], sess.Idle[1:]
					req.detach()
					go reply(req, w)
					continue outer
				}
				reqSessions = append(reqSessions, sess)
			}
			if len(reqSessions) == 0 {
				break
			}
			w := &worker{
				Candidates: reqSessions,
				returnc:    workerc,
			}
			nworker++
			nstarted++
			go w.Start(ctx)
		case w := <-workerc:
			if err := w.Err(); err != nil {
				// TODO(marius): allocate a new worker to replace this one.
				if w.Session != nil {
					panic(fmt.Sprintf("nonnil session, %v %v", w, w.Err()))
				}
				nworker--
				log.Error.Printf("worker %s error: %v", w, err)
				break
			}
			if w.Session == nil {
				panic(fmt.Sprintf("nil session, %v %v", w, w.Err()))
			}
			sess := w.Session
			if len(sess.Requests) > 0 {
				for req := range sess.Requests {
					req.detach()
					go reply(req, w)
					continue outer
				}
				panic("should not reach here")
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
	pctx := ctx
	ctx, cancel := context.WithCancel(ctx)
	config, err := study.Run(values)
	if err != nil {
		return diviner.Run{}, err
	}
	if _, err := r.db.CreateStudyIfNotExist(ctx, study); err != nil {
		return diviner.Run{}, err
	}
	run := new(run)
	run.Run, err = r.db.InsertRun(ctx, diviner.Run{Study: study.Name, Values: values, Config: config})
	if err != nil {
		return diviner.Run{}, err
	}
	run.Study = study
	run.Values = values
	run.Config = config
	r.add(run)
	defer r.remove(run)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tick := time.NewTicker(keepaliveInterval)
		defer tick.Stop()
		defer wg.Done()
		for {
			select {
			case <-tick.C:
			case <-ctx.Done():
				return
			}
			status, message, elapsed := run.Status()
			if err := r.db.UpdateRun(ctx, study.Name, run.Run.Seq, diviner.Pending, fmt.Sprintf("%s: %s", status, message), elapsed); err != nil && err != context.Canceled {
				log.Error.Printf("run %s:%d: error setting status: %v", run.Run.Study, run.Run.Seq, message)
			}
		}
	}()
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
	cancel()
	ctx = pctx
	wg.Wait()
	status, message, elapsed = run.Status()
	if err := r.db.UpdateRun(ctx, study.Name, run.Run.Seq, state, "", elapsed); err != nil {
		log.Error.Printf("run %s:%d: error setting status: %v", run.Run.Study, run.Run.Seq, message)
		return diviner.Run{}, err
	}
	// Refresh the run status before we return it.
	run.Run, err = r.db.LookupRun(ctx, study.Name, run.Run.Seq)
	return run.Run, err
}

func (r *Runner) Round(ctx context.Context, study diviner.Study, ntrials int) (done bool, err error) {
	complete, err := r.db.ListRuns(ctx, study.Name, diviner.Success, time.Time{})
	if err != nil && err != diviner.ErrNotExist {
		return false, err
	}
	trials := make([]diviner.Trial, len(complete))
	for i, run := range complete {
		trials[i] = run.Trial()
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
		if run.State != diviner.Success {
			return false, nil
		}
	}
	return ntrials == 0 || (len(runs) < ntrials), nil
}

// Allocate allocates a new worker and returns it. Workers must
// be returned after they are done by calling w.Return.
func (r *Runner) allocate(ctx context.Context, sys []*diviner.System) (*worker, error) {
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

	// Sys is the list of systems from which a new machine may be allocated.
	sys []*diviner.System

	// Sessions is the list of sessions that this request is waiting on.
	// len(sys)==len(sessions).  This field and session.Requests link to each
	// other.
	sessions []*session
}

func newRequest(sys []*diviner.System) *request {
	return &request{make(chan *worker), sys, nil}
}

// Remove this request from all the sessions that it's waiting on.
func (r *request) detach() {
	for _, sess := range r.sessions {
		if _, ok := sess.Requests[r]; !ok {
			panic("request not found")
		}
		delete(sess.Requests, r)
	}
	r.sessions = nil
}

// Register this request to the session's waitlist.
func (r *request) attach(sess *session) {
	if _, ok := sess.Requests[r]; ok {
		panic("duplicate request")
	}
	sess.Requests[r] = struct{}{}
	r.sessions = append(r.sessions, sess)
}

func (r *request) Reply() <-chan *worker {
	return r.replyc
}

// Session stores the bigmachine session and associated state for a
// single system.
type session struct {
	// System describes the system from which this session is started.
	System *diviner.System
	// B stores the bigmachine sessions associated with this system.
	B *bigmachine.B

	// Requests is the pending requests for workers in this system.  This field
	// and request.session link to each other.
	Requests map[*request]struct{}
	// Idle is the set of idle workers in this system.
	Idle []*worker

	mu sync.Mutex
	// # of workers running in this session.
	//
	// INVARIANT: System.Pararallelism == 0 || nWorker <= System.Parallelism
	nWorker int
}

func (s *session) tryAcquire() bool {
	if s.System.Parallelism <= 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nWorker >= s.System.Parallelism {
		return false
	}
	s.nWorker++
	return true
}

func (s *session) release() {
	if s.System.Parallelism <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nWorker--
	if s.nWorker < 0 {
		panic(s)
	}
}
