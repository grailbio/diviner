// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package dydb

import (
	"testing"
	"time"
)

func TestDates(t *testing.T) {
	now := time.Now()
	if got, want := dates(now.Add(-48*time.Hour), now), 2; len(got) < want {
		t.Errorf("got %v, want %v", got, want)
	}

	var (
		beg = time.Date(2019, 03, 21, 22, 0, 0, 0, time.UTC)
		end = time.Date(2019, 03, 22, 2, 0, 0, 0, time.UTC)
	)
	interval := dates(beg, end)
	if got, want := len(interval), 2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := interval[0], time.Date(2019, 03, 21, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := interval[1], time.Date(2019, 03, 22, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	interval = dates(beg, beg)
	if got, want := len(interval), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := interval[0], time.Date(2019, 03, 21, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	interval = dates(beg, beg.Add(time.Hour))
	if got, want := len(interval), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := interval[0], time.Date(2019, 03, 21, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
