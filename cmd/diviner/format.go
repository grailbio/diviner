// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"strings"
	"unicode"
)

// Reindent re-indents a block of text (which can contain multiple lines),
// replacing a shared whitespace prefix with the provided one.
func reindent(indent, block string) string {
	lines := strings.Split(block, "\n")
	// Trim empty prefix and suffix lines.
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	// Strip common prefix
	switch len(lines) {
	case 0:
	case 1:
		lines[0] = strings.TrimSpace(lines[0])
	default:
		// The first line can't be empty because we trim them above,
		// so this determines our prefix.
		prefix := spacePrefix(lines[0])
		for i := range lines {
			lines[i] = strings.TrimRightFunc(lines[i], unicode.IsSpace)
			// Skip empty lines; they shouldn't be able to mess up our prefix.
			if lines[i] == "" {
				continue
			}
			if !strings.HasPrefix(lines[i], prefix) {
				linePrefix := spacePrefix(lines[i])
				if strings.HasPrefix(prefix, linePrefix) {
					prefix = linePrefix
				} else {
					prefix = ""
				}
			}
		}
		for i, line := range lines {
			lines[i] = strings.TrimPrefix(line, prefix)
		}
	}
	if len(lines) == 0 {
		return ""
	}
	return indent + strings.Join(lines, "\n"+indent)
}

func spacePrefix(s string) string {
	var prefix string
	for _, r := range s {
		if !unicode.IsSpace(r) {
			break
		}
		prefix += string(r)
	}
	return prefix
}
