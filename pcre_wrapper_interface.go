// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

type PcreWrapperInterface interface {
	Match(b []byte) bool
}
