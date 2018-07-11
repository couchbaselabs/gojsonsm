// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"strconv"
)

type fastLitParser struct {
	tmpInt       int64
	tmpNum       float64
	tmpBool      bool
	tmpBytes     []byte
	tmpBytesData [64]byte
}

func (p *fastLitParser) ParseInt(bytes []byte) int64 {
	var v int64

	if len(bytes) == 0 {
		return 0
	}

	var neg bool = false
	if bytes[0] == '-' {
		neg = true
		bytes = bytes[1:]
	}

	for _, c := range bytes {
		if c >= '0' && c <= '9' {
			v = (10 * v) + int64(c-'0')
		} else {
			return 0
		}
	}

	if neg {
		return -v
	} else {
		return v
	}
}

func (p *fastLitParser) ParseNumber(bytes []byte) float64 {
	val, _ := strconv.ParseFloat(string(bytes), 64)
	return val
}

func (p *fastLitParser) ParseString(bytes []byte) []byte {
	return bytes[1 : len(bytes)-1]
}

func (p *fastLitParser) ParseEscString(bytes []byte) []byte {
	bytesOut, _ := unescapeJsonString(bytes[1:len(bytes)-1], p.tmpBytesData[:])
	return bytesOut
}

func (p *fastLitParser) Parse(token TokenType, bytes []byte) FastVal {
	switch token {
	case tknString:
		return NewBinStringFastVal(p.ParseString(bytes))
	case tknEscString:
		return NewBinaryFastVal(p.ParseEscString(bytes))
	case tknInteger:
		return NewIntFastVal(p.ParseInt(bytes))
	case tknNumber:
		return NewFloatFastVal(p.ParseNumber(bytes))
	case tknNull:
		return NewNullFastVal()
	case tknTrue:
		return NewBoolFastVal(true)
	case tknFalse:
		return NewBoolFastVal(false)
	}

	panic("invalid token")
}
