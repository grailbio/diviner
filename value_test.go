// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner_test

import (
	"testing"

	"github.com/grailbio/diviner"
)

func TestValue(t *testing.T) {
	if diviner.Int(1) != diviner.Int(1) {
		t.Error("integers not comparable")
	}
	if diviner.Float(12.3) != diviner.Float(12.3) {
		t.Error("floats not comparable")
	}
	if diviner.String("xyz") != diviner.String("xyz") {
		t.Error("strings not comparable")
	}
}
