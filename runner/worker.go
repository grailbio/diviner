// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"encoding/gob"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/bigmachine"
	"golang.org/x/time/rate"
)

// Preamble is prepended to any script run by the worker. It is exposed
// here for testing.
//
// TODO(marius): make this unnecessary by fixing bigmachine.
var Preamble = `set -ex; su - ubuntu; export HOME=/home/ubuntu; `

var (
	machineRetry = retry.Jitter(retry.Backoff(30*time.Second, 5*time.Minute, 1.5), 0.5)
	machineLimit = rate.NewLimiter(rate.Limit(0.5), 3)
	allocating   = expvar.NewInt("allocating")
	allocated    = expvar.NewInt("allocated")
)

// A worker represents a single bigmachine worker in Diviner. It
// provides functions to run commands using the command service, and
// to return the worker to the main event loop.
type worker struct {
	*bigmachine.Machine

	IdleTime time.Time

	returnc chan<- *worker
	err     error
}

// StartWorker starts a new worker using the provided bigmachine
// session. Once the worker has started (or failed to start), it is
// returned on returnc.
func startWorker(ctx context.Context, b *bigmachine.B, returnc chan<- *worker) {
	w := &worker{returnc: returnc}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()

	// First try to allocate a machine. We keep a jittered retry schedule so that
	// we have a chance to get through the various rate limits.
	allocating.Add(1)
	defer allocating.Add(-1)
	defer w.Return()
	for try := 0; ; try++ {
		// TODO(marius): distinguish between true allocation errors
		// and others that may occur.
		if err := machineLimit.Wait(ctx); err != nil {
			w.err = err
			return
		}
		machines, err := b.Start(ctx, 1, bigmachine.Services{
			"Cmd": &commandService{},
		})
		if err == nil && len(machines) == 0 {
			err = errors.New("no machines allocated")
		} else if err == nil {
			w.Machine = machines[0]
			select {
			case <-ctx.Done():
				w.err = ctx.Err()
				return
			case <-time.After(5 * time.Minute):
				err = errors.New("timeout while waiting for machine to start")
			case <-w.Wait(bigmachine.Running):
				if w.State() != bigmachine.Running {
					err = fmt.Errorf("machine failed to start: %v", w.Err())
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
			w.err = err
			return
		}
	}
	if w.err == nil {
		allocated.Add(1)
		log.Printf("allocated machine %s in %s", w.Addr, time.Since(start))
	}
}

// String returns a textual representation of worker w.
func (w *worker) String() string {
	if w.Machine == nil {
		return "unallocated"
	}
	return fmt.Sprintf("%s (%s)", w.Addr, w.State())
}

// Reset resets the worker's state, erasing the contents of the
// command working space.
func (w *worker) Reset(ctx context.Context) error {
	return w.Call(ctx, "Cmd.Reset", struct{}{}, nil)
}

// CopyFiles copies a set of local files to the worker's command
// working space.
func (w *worker) CopyFiles(ctx context.Context, files []string) error {
	for _, path := range files {
		file := fileLiteral{Name: path}
		var err error
		file.Contents, err = ioutil.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read local file %s: %v", path, err)
		}
		if err := w.Call(ctx, "Cmd.WriteFile", file, nil); err != nil {
			return fmt.Errorf("failed to upload file %s: %v", path, err)
		}
	}
	return nil
}

// Run runs the provided script using the Bash shell interpreter. The
// current working directory is set to the worker's command working
// space. The returned io.ReadCloser is the processes' standard
// output and standard error.
func (w *worker) Run(ctx context.Context, script string) (io.ReadCloser, error) {
	var out io.ReadCloser
	// TODO(marius): allow bigmachine to run under the default user.
	script = Preamble + script
	err := w.Call(ctx, "Cmd.Run", []string{"bash", "-c", script}, &out)
	return out, err
}

// Return returns this worker to the main event loop.
func (w *worker) Return() {
	w.returnc <- w
}

// Err returns the worker's error condition. If non-nil, the worker
// is unhealthy and should be discarded.
func (w *worker) Err() error {
	if w.err != nil {
		return w.err
	}
	return w.Machine.Err()
}

func init() {
	gob.Register(&commandService{})
}

// CommandService is a simple bigmachine service to run commands. A
// commandService always has a workspace, which may be reset by
// calling Reset.
type commandService struct {
	// Gob needs at least one exported field.
	ExportedForGob int
	dir            string
}

func (c *commandService) Init(_ *bigmachine.B) error {
	var err error
	c.dir, err = ioutil.TempDir("", "command")
	return err
}

// FileLiteral represents a file's name and contents.
type fileLiteral struct {
	Name     string
	Contents []byte
}

// Reset resets the current workspace, removing all files in the CWD
// of commands run by the service.
func (c *commandService) Reset(ctx context.Context, _ struct{}, _ *struct{}) error {
	if c.dir != "" {
		_ = os.RemoveAll(c.dir)
	}
	var err error
	c.dir, err = ioutil.TempDir("", "command")
	return err
}

// WriteFile writes the provided file into the workspace.
func (c *commandService) WriteFile(ctx context.Context, file fileLiteral, _ *struct{}) error {
	return ioutil.WriteFile(filepath.Join(c.dir, file.Name), file.Contents, 0644)
}

// Run runs a command in the workspace. Its standard output and error are
// streamed to the provided ReadCloser.
//
// TODO(marius): multiplex the streams to separate standard output from
// standard error.
func (c *commandService) Run(ctx context.Context, command []string, reply *io.ReadCloser) error {
	if len(command) == 0 {
		return errors.New("empty command")
	}
	r, w := io.Pipe()
	go func() {
		cmd := exec.Command(command[0], command[1:]...)
		cmd.Stdout = w
		cmd.Stderr = w
		cmd.Dir = c.dir
		w.CloseWithError(cmd.Run())
	}()
	*reply = r
	return nil
}
