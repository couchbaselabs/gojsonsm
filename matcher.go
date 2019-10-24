// Copyright 2019 Couchbase, Inc. All rights reserved.

package gojsonsm

const (
	MatcherNoStatus    = 0x0
	MatcherCollateUsed = 0x1
)

type Matcher interface {
	Match([]byte) (bool, error)
	MatchWithStatus([]byte) (bool, int, error)
	Reset()
}
