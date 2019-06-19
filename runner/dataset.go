// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/diviner"
)

// A dataset represents a process that creates a datataset, which
// runs may depend on.
type dataset struct {
	diviner.Dataset

	donec chan struct{}

	mu     sync.Mutex
	status status
	err    error
}

// NewDataset creates a new runnable dataset from a diviner dataset
// configuration.
func newDataset(d diviner.Dataset) *dataset {
	return &dataset{
		Dataset: d,
		donec:   make(chan struct{}),
	}
}

// Do processes the dataset, possibly allocating a worker from the
// provided runner. Upon return, the dataset's status must be done.
func (d *dataset) Do(ctx context.Context, runner *Runner) {
	// First check if the dataset already exists.
	if url := d.IfNotExist; url != "" {
		if stat, err := file.Stat(ctx, url); err == nil {
			Logger.Printf("dataset %s: found %s, with modtime %v", d.Name, url, stat.ModTime())
			d.setStatus(statusOk)
			return
		} else if !errors.Is(errors.NotExist, err) {
			d.error(errors.E("dataset: ifnotexist", url, err))
			return
		}
	}
	Logger.Printf("dataset %s: %s not found, start data generation", d.Name, d.IfNotExist)
	w, err := runner.allocate(ctx, d.Systems)
	if err != nil {
		d.error(errors.E("dataset: allocate", d.Systems, err))
		return
	}
	defer w.Return()
	d.setStatus(statusRunning)
	// First clean up the workspace.
	if err := w.Reset(ctx); err != nil {
		d.error(err)
		return
	}
	if err := w.CopyFiles(ctx, d.LocalFiles); err != nil {
		d.error(errors.E(fmt.Sprintf("dataset copyfiles %+v: %v", d.LocalFiles, err)))
		return
	}
	out, err := w.Run(ctx, d.Script, nil)
	if err != nil {
		d.error(errors.E(fmt.Sprintf("dataset: failed to start script '%s'", d.Script), err))
		return
	}
	var writer io.Writer = ioutil.Discard
	path := fmt.Sprintf("dataset.%s.log", d.Name)
	path = strings.Replace(path, "/", "_", -1)
	if f, err := os.Create(path); err == nil {
		writer = f
		defer f.Close()
	} else {
		log.Error.Printf("dataset: create %s: %v", path, err)
	}
	_, err = io.Copy(writer, out)
	if e := out.Close(); e != nil && err == nil {
		err = e
	}
	if err == nil {
		d.setStatus(statusOk)
	} else {
		d.error(err)
	}
}

// Done returns a channel that is closed when the dataset run
// completes.
func (d *dataset) Done() <-chan struct{} {
	return d.donec
}

// Status returns the dataset's current status.
func (d *dataset) Status() status {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.status
}

func (d *dataset) setStatus(status status) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.setStatusLocked(status)
}

// SetStatus sets the datasets current status.
func (d *dataset) setStatusLocked(status status) {
	done := !d.status.Done() && status.Done()
	d.status = status
	if done {
		close(d.donec)
	}
}

// Error sets the dataset's status to statusErr and
// its error to the provided error.
func (d *dataset) error(err error) {
	log.Error.Printf("dataset %s: %v", d.Name, err)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.err = err
	d.setStatusLocked(statusErr)
}

func (d *dataset) errorf(format string, v ...interface{}) {
	d.error(fmt.Errorf(format, v...))
}

// Err returns the dataset's error, if any.
func (d *dataset) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

// String returns a textual representation of this dataset.
func (d *dataset) String() string {
	return fmt.Sprintf("%s (%s)", d.Dataset.Name, d.Status())
}
