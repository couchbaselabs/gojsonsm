// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

type TokenType int

const (
	tknUnknown TokenType = iota
	tknObjectStart
	tknArrayStart
	tknObjectEnd
	tknArrayEnd
	tknString
	tknEscString
	tknInteger
	tknNumber
	tknNull
	tknTrue
	tknFalse
	tknEnd
)

func isLiteralToken(token TokenType) bool {
	return token >= tknString && token <= tknFalse
}

func tokenToText(token TokenType) string {
	switch token {
	case tknUnknown:
		return "unknown"
	case tknObjectStart:
		return "object_start"
	case tknArrayStart:
		return "array_start"
	case tknObjectEnd:
		return "object_end"
	case tknArrayEnd:
		return "array_end"
	case tknString:
		return "string"
	case tknEscString:
		return "escaped_string"
	case tknInteger:
		return "integer"
	case tknNumber:
		return "number"
	case tknNull:
		return "null"
	case tknTrue:
		return "true"
	case tknFalse:
		return "false"
	case tknEnd:
		return "end"
	}
	return "??ERROR??"
}

type jsonTokenizer struct {
	scanner jsonScanner
	pos     int
	data    []byte

	replayState int
}

func (tok *jsonTokenizer) start(data []byte) {
	tok.replayState = scanContinue
	tok.pos = 0
	tok.data = data
	tok.scanner.reset()
}

func (tok *jsonTokenizer) seek(pos int) {
	tok.replayState = scanContinue
	tok.pos = pos
	tok.scanner.reset()
}

func (tok *jsonTokenizer) scanStep() int {
	if tok.pos >= len(tok.data) {
		return tok.scanner.eof()
	}

	state := tok.scanner.step(&tok.scanner, tok.data[tok.pos])
	tok.pos++
	return state
}

func (tok *jsonTokenizer) step() (TokenType, []byte, error) {
	start := tok.pos
	for {
		state := tok.scanStep()
		switch state {
		case scanError:
			return 0, nil, tok.scanner.err
		case scanSkipSpace:
			start = tok.pos
		case scanBeginLiteral:
			for {
				state := tok.scanStep()
				if state == scanError {
					return 0, nil, tok.scanner.err
				} else if state == scanContinue {
					continue
				} else {
					tok.scanner.undo(state)
					tok.pos--
					bytes := tok.data[start:tok.pos]
					switch tok.scanner.litType {
					case litString:
						return tknString, bytes, nil
					case litEscString:
						return tknEscString, bytes, nil
					case litInteger:
						return tknInteger, bytes, nil
					case litNumber:
						return tknNumber, bytes, nil
					case litNull:
						return tknNull, bytes, nil
					case litTrue:
						return tknTrue, bytes, nil
					case litFalse:
						return tknTrue, bytes, nil
					default:
						panic("unexpected token literal type")
					}
				}
			}

		case scanBeginObject:
			return tknObjectStart, nil, nil
		case scanBeginArray:
			return tknArrayStart, nil, nil
		case scanEndObject:
			return tknObjectEnd, nil, nil
		case scanEndArray:
			return tknArrayEnd, nil, nil
		case scanContinue:
			panic("unexpected continue")
		case scanObjectKey:
			start = tok.pos
		case scanObjectValue:
			start = tok.pos
		case scanArrayValue:
			start = tok.pos
		case scanEnd:
			return tknEnd, nil, nil
		}
	}
}
