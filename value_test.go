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

func TestList(t *testing.T) {
	l1 := &diviner.List{diviner.Int(10), diviner.Int(20)}
	l2 := &diviner.List{diviner.Int(5), diviner.Int(20)}
	if l1.Less(l1) {
		t.Error("l1.Less(l1)")
	}
	if !l2.Less(l1) {
		t.Error("expected l2.Less(l1)")
	}
}
