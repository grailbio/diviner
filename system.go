// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner

import (
	"github.com/grailbio/bigmachine"
	"go.starlark.net/starlark"
)

// A System is used to create bigmachine sessions that is used
// to create machines on which trials and datasets are run.
type System struct {
	// System is the bigmachine system configured by this
	// system.
	bigmachine.System
	// ID is a unique identifier for this system.
	ID string
	// Parallelism specifies the maximum level of job parallelism
	// allowable for this system.
	Parallelism int
}

// String implements starlark.Value.
func (s *System) String() string { return s.ID }

// Type implements starlark.Value.
func (*System) Type() string { return "system" }

// Freeze implements starlark.Value.
func (*System) Freeze() {}

// Truth implements starlark.Value.
func (*System) Truth() starlark.Bool { return true }

// Hash implements starlark.Value.
func (s *System) Hash() (uint32, error) { return 0, errNotHashable }
