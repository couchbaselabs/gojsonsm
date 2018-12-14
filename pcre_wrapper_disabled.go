// Copyright 2018 Couchbase, Inc. All rights reserved.

// +build !pcre

package gojsonsm

type PcreWrapper struct {
}

func MakePcreWrapper(expression string) (PcreWrapperInterface, error) {
	return &PcreWrapper{}, ErrorPcreNotSupported
}

func (wrapper *PcreWrapper) Match(b []byte) bool {
	return false
}

func MakePcreExpression(expression string) (Expression, error) {
	return nil, ErrorPcreNotSupported
}
