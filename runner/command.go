// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"encoding/gob"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/grailbio/bigmachine"
)

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
	// Name is the name under which the file is installed in the
	// command service workspace.
	Name string
	// Contents is the file's contents.
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
		// TODO(marius): this does not appear to always properly report failures.
		cmd := exec.Command(command[0], command[1:]...)
		cmd.Stdout = w
		cmd.Stderr = w
		cmd.Dir = c.dir
		w.CloseWithError(cmd.Run())
	}()
	*reply = r
	return nil
}
