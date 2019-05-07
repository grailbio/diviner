// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/grailbio/diviner"
)

func TestMap(t *testing.T) {
	const N = 1000
	var (
		m    = diviner.NewMap()
		vals = make([]diviner.Value, N)
	)
	for i := range vals {
		var v diviner.Value
		switch i % 3 {
		case 0:
			v = diviner.Int(i)
		case 1:
			v = diviner.String(fmt.Sprint(i))
		case 2:
			v = diviner.List{diviner.Int(i), diviner.Int(i - 1)}
		}
		vals[i] = v
	}
	rand.Shuffle(len(vals), func(i, j int) { vals[i], vals[j] = vals[j], vals[i] })

	for _, val := range vals {
		m.Put(val, val)
	}

	for _, val := range vals {
		v, ok := m.Get(val)
		if !ok {
			t.Errorf("missing entry for key %s", val)
			continue
		}
		if got, want := v.(diviner.Value), val; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	m.Put(vals[0], "hello world")
	if v, ok := m.Get(vals[0]); !ok {
		t.Errorf("missing key %v", vals[0])
	} else if got, want := v.(string), "hello world"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	var n int
	m.Range(func(diviner.Value, interface{}) {
		n++
	})
	if got, want := n, N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := m.Len(), N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
