// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package script

import (
	"fmt"
)

type Database int

const (
	Local Database = iota
	DynamoDB
)

func (d Database) String() string {
	switch d {
	case Local:
		return "local"
	case DynamoDB:
		return "dynamodb"
	default:
		panic(d)
	}
}

type Config struct {
	Database Database
	Table    string
}

func (c Config) String() string {
	return fmt.Sprintf("config(database=%s, table=%s)", c.Database, c.Table)
}
