// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/diviner"
	"golang.org/x/sync/errgroup"
)

// A Streamer is used to control streaming trials.
type Streamer struct {
	runner    *Runner
	study     diviner.Study
	nparallel int
	stopc     chan struct{}
	donec     chan error
}

// Stream starts a streaming run for the provided study and with the
// provided parallelism. The returned Streamer controls the ongoing
// study. Streamers maintain the target parallelism, requesting new
// points from the underlying oracle as they are needed. Streaming
// studies stop when they are requested by the caller, or after running
// out of points to explore, as determined by the study's oracle.
func (r *Runner) Stream(ctx context.Context, study diviner.Study, nparallel int) *Streamer {
	s := &Streamer{
		runner:    r,
		study:     study,
		nparallel: nparallel,
		stopc:     make(chan struct{}),
		donec:     make(chan error),
	}
	go func() {
		s.donec <- s.do(ctx)
	}()
	return s
}

type runRequest struct {
	Index int
	diviner.Values
	diviner.Replicates
}

type runResponse struct {
	Index int
	diviner.Trial
	Err error
}

func (s *Streamer) do(ctx context.Context) error {
	nreplicate := s.study.Replicates
	if nreplicate == 0 {
		nreplicate = 1
	}
	var (
		reqs  = make(chan runRequest)
		resps = make(chan runResponse, s.nparallel)
		wg    sync.WaitGroup
	)
	wg.Add(s.nparallel)
	defer wg.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer close(reqs)
	for i := 0; i < s.nparallel; i++ {
		go func() {
			defer wg.Done()
			for req := range reqs {
				var (
					mu   sync.Mutex
					runs []diviner.Run
				)
				g, ctx := errgroup.WithContext(ctx)
				for replicate := 0; replicate < nreplicate; replicate++ {
					if req.Replicates.Contains(replicate) {
						continue
					}
					replicate := replicate
					g.Go(func() error {
						run, err := s.runner.Run(ctx, s.study, req.Values, int(replicate))
						if err == nil {
							mu.Lock()
							runs = append(runs, run)
							mu.Unlock()
						}
						return err
					})
				}
				if err := g.Wait(); err != nil {
					resps <- runResponse{Index: req.Index, Err: err}
					continue
				}
				trials := make([]diviner.Trial, len(runs))
				for i := range trials {
					trials[i] = runs[i].Trial()
				}
				resps <- runResponse{Index: req.Index, Trial: diviner.ReplicatedTrial(trials)}
			}
		}()
	}

	var (
		npending int
		valueq   []diviner.Values
		trials   []diviner.Trial
		done     bool
		stopc    = s.stopc
	)
	// We query the database once at the beginning and then maintain our
	// own set of running trials. This helps us reduce database load but
	// it also simplifies the consistency model: the set of trials we
	// maintain are exactly the ones we have launched, etc.
	initTrials, err := diviner.Trials(ctx, s.runner.db, s.study, diviner.Success|diviner.Pending)
	if err != nil {
		return err
	}
	initTrials.Range(func(_ diviner.Value, v interface{}) {
		trial := v.(diviner.Trial)
		if trial.Replicates.Completed(s.study.Replicates) {
			trials = append(trials, trial)
		}
	})
	for !done || npending > 0 {
		// Drain responses in case they are queued.
		select {
		case resp := <-resps:
			npending--
			if resp.Err != nil {
				return resp.Err
			}
			trials[resp.Index] = resp.Trial
			continue
		default:
		}

		if n := s.nparallel - npending; !done && len(valueq) == 0 && n > 0 {
			log.Printf("%s: requesting %d new points from oracle from %d trials (streaming)", s.study.Name, n, len(trials))
			// TODO(marius): it may be useful to request more points
			// than we can immediately fill, especially for expensive oracles.
			// Alternatively, we could make oracle stateful.
			var err error
			valueq, err = s.study.Oracle.Next(trials, s.study.Params, s.study.Objective, n)
			if err != nil {
				return err
			}
			done = len(valueq) < n
		}

		var (
			reqc chan runRequest
			req  runRequest
		)
		if len(valueq) > 0 {
			reqc = reqs
			req.Index = len(trials)
			req.Values = valueq[0]
			if v, ok := initTrials.Get(valueq[0]); ok {
				req.Replicates = v.(diviner.Trial).Replicates
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case reqc <- req:
			trials = append(trials, diviner.Trial{Values: valueq[0], Pending: true})
			valueq = valueq[1:]
			npending++
		case resp := <-resps:
			npending--
			if resp.Err != nil {
				return resp.Err
			}
			trials[resp.Index] = resp.Trial
		case <-stopc:
			done = true
			stopc = nil
		}
	}
	return nil
}

// Stop requests that the streaming study should stop after currently
// executing trials complete. The study has stopped only after the
// Wait method returns.
func (s *Streamer) Stop() {
	close(s.stopc)
}

// Wait blocks until the study has completed. If the study fails, an
// error is returned.
func (s *Streamer) Wait() error {
	return <-s.donec
}
