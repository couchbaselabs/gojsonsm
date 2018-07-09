// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"testing"
)

var globalParse fastLitParser

var testString = []byte(`"test"`)
var testBigString = []byte(`"1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890"`)
var testEscString = []byte(`"te\n\tst"`)
var testBigEscString = []byte(`"1234567890\t1234567890\t1234567890\t1234567890\t1234567890\t1234567890\t1234567890"`)
var testInteger = []byte(`14322`)
var testNumber = []byte(`14.2`)
var testNullBytes = []byte(`null`)
var testTrueBytes = []byte(`true`)
var testFalseBytes = []byte(`false`)

func BenchmarkParseString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		globalParse.Parse(tknString, testString)
	}
}

func BenchmarkParseBigString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		globalParse.Parse(tknString, testBigString)
	}
}

func BenchmarkParseEscString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		globalParse.Parse(tknEscString, testEscString)
	}
}

func BenchmarkParseBigEscString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		globalParse.Parse(tknEscString, testBigEscString)
	}
}

func BenchmarkParseInteger(b *testing.B) {
	for i := 0; i < b.N; i++ {
		globalParse.Parse(tknInteger, testInteger)
	}
}

func BenchmarkParseNumber(b *testing.B) {
	for i := 0; i < b.N; i++ {
		globalParse.Parse(tknNumber, testNumber)
	}
}

func BenchmarkParseNull(b *testing.B) {
	for i := 0; i < b.N; i++ {
		globalParse.Parse(tknNull, testNullBytes)
	}
}

func BenchmarkParseTrue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		globalParse.Parse(tknTrue, testTrueBytes)
	}
}

func BenchmarkParseFalse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		globalParse.Parse(tknFalse, testFalseBytes)
	}
}
