// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package diviner

import "sort"

// SortValues sorts the provided set of values by keys. It assumes
// that all of the values have exactly the same sets of keys.
func SortValues(vs []Values) {
	if len(vs) == 0 {
		return
	}
	keys := make([]string, 0, len(vs[0]))
	for key := range vs[0] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	sort.SliceStable(vs, func(i, j int) bool {
		for _, key := range keys {
			switch {
			case vs[i][key].Less(vs[j][key]):
				return true
			case vs[j][key].Less(vs[i][key]):
				return false
			}
		}
		return false
	})
}
