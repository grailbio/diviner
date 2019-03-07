// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package runner

import (
	"context"
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

func TestCommand(t *testing.T) {
	ctx := context.Background()
	var c commandService
	if err := c.Init(nil); err != nil {
		t.Fatal(err)
	}

	var out io.ReadCloser
	if err := c.Run(ctx, cmd{Args: []string{"bash", "-c", "exit 1"}}, &out); err != nil {
		t.Fatal(err)
	}
	_, err := io.Copy(ioutil.Discard, out)
	if err == nil || !strings.Contains(err.Error(), "exit status 1") {
		t.Errorf("bad error %s", err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}
