// Copyright 2019 Couchbase, Inc. All rights reserved.

package gojsonsm

type Matcher interface {
	Match([]byte) (bool, error)
	Reset()
}
