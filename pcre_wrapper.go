// Copyright 2018 Couchbase, Inc. All rights reserved.

// +build pcre

package gojsonsm

import (
	"errors"
	"github.com/glenn-brown/golang-pkg-pcre/src/pkg/pcre"
)

type PcreWrapper struct {
	pcreRegex *pcre.Regexp
}

func MakePcreWrapper(expression string) (PcreWrapperInterface, error) {
	pcreWrapper := &PcreWrapper{}

	pcreRegex, err := pcre.Compile(expression, 0)
	if err != nil {
		return pcreWrapper, errors.New("failed to compile PcreExpr: " + err.Message)
	}
	pcreWrapper.pcreRegex = &pcreRegex

	return pcreWrapper, nil
}

func (wrapper *PcreWrapper) Match(b []byte) bool {
	matcher := pcre.Matcher{}
	matcher.Reset(*wrapper.pcreRegex, b, 0)
	return matcher.Matches()
}

func MakePcreExpression(expression string) (Expression, error) {
	return PcreExpr{expression}, nil
}
