// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
)

// Function related constants
const (
	MathFuncAbs   string = "mathAbs"
	MathFuncAcos  string = "mathFuncAcos"
	MathFuncAsin  string = "mathFuncAsin"
	MathFuncAtan  string = "mathFuncAtan"
	MathFuncCeil  string = "mathCeil"
	MathFuncCos   string = "mathCos"
	MathFuncExp   string = "mathFuncExp"
	MathFuncFloor string = "mathFuncFloor"
	MathFuncLog   string = "mathFuncLog"
	MathFuncLn    string = "mathFuncLn"
	MathFuncRound string = "mathRound"
	MathFuncSin   string = "mathSin"
	MathFuncSqrt  string = "mathSqrt"
	MathFuncTan   string = "mathTan"
)

// Parser related constants
// Error constants
var emptyExpression Expression
var ErrorNotFound error = fmt.Errorf("Error: Specified resource was not found")
var ErrorNoMoreTokens error = fmt.Errorf("Error: No more token found")
var ErrorNeedToStartOneNewCtx error = fmt.Errorf("Error: Need to spawn one subcontext")
var ErrorNeedToStartNewCtx error = fmt.Errorf("Error: Need to spawn subcontext")
var ErrorParenMismatch error = fmt.Errorf("Error: Parenthesis mismatch")
var NonErrorOneLayerDone error = fmt.Errorf("One layer has finished")
var ErrorLeadingZeroes error = fmt.Errorf("Nested mode index must not have leading zeros")
var ErrorAllInts error = fmt.Errorf("Array index must be a valid integer")
var ErrorEmptyNest error = fmt.Errorf("Array index cannot be empty")
var ErrorMissingBacktickBracket error = fmt.Errorf("Invalid field - could not find matching ending backtick or bracket")
var ErrorMissingQuote error = fmt.Errorf("Invalid token - could not find matching ending quote")
var ErrorEmptyLiteral error = fmt.Errorf("Literals cannot be empty")
var ErrorEmptyToken error = fmt.Errorf("Token cannot be empty")
var ErrorInvalidFuncArgs error = fmt.Errorf("Unable to parse arguments to specified built in function")
var ErrorPcreNotSupported error = fmt.Errorf("Error: Current instance of gojsonsm does not have native PCRE support compiled")
var ErrorFieldPathNotFound error = fmt.Errorf("Error: Unable to find internally stored field path")
var ErrorMalformedFxInternals error = fmt.Errorf("Error: Malformed internal function helper")
var ErrorMalformedParenthesis error = fmt.Errorf("Invalid parenthesis case")

// Parse mode is within the context that a valid expression should be generically of the type of:
// field > op -> value -> chain, repeat.
type parseMode int

const (
	invalidMode parseMode = iota
	fieldMode   parseMode = iota
	opMode      parseMode = iota
	valueMode   parseMode = iota
	chainMode   parseMode = iota
)

func (pm parseMode) String() string {
	switch pm {
	case fieldMode:
		return "fieldMode"
	case opMode:
		return "opMode"
	case valueMode:
		return "valueMode"
	case chainMode:
		return "chainMode"
	default:
		return "Unknown"
	}
}

const (
	fieldSeparator   string = "."
	fieldLiteral     string = "`"
	fieldNestedStart string = "["
	fieldNestedEnd   string = "]"
)

// When in op mode, there can be multiple contexts
type opTokenContext int

const (
	noOp      opTokenContext = iota
	chainOp   opTokenContext = iota
	compareOp opTokenContext = iota
	matchOp   opTokenContext = iota
	noFieldOp opTokenContext = iota
)
