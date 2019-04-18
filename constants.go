// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
	"regexp"
)

// Function related constants
const (
	DateFunc        string = "date"
	MathFuncAbs     string = "mathAbs"
	MathFuncAcos    string = "mathAcos"
	MathFuncAsin    string = "mathAsin"
	MathFuncAtan    string = "mathAtan"
	MathFuncAtan2   string = "mathAtan2"
	MathFuncCeil    string = "mathCeil"
	MathFuncCos     string = "mathCos"
	MathFuncDegrees string = "mathDegrees"
	MathFuncE       string = "mathE"
	MathFuncExp     string = "mathExp"
	MathFuncFloor   string = "mathFloor"
	MathFuncLog     string = "mathLog"
	MathFuncLn      string = "mathLn"
	MathFuncPi      string = "mathPi"
	MathFuncPow     string = "mathPow"
	MathFuncRadians string = "mathRadians"
	MathFuncRound   string = "mathRound"
	MathFuncSin     string = "mathSin"
	MathFuncSqrt    string = "mathSqrt"
	MathFuncTan     string = "mathTan"
	MathFuncAdd     string = "mathAdd"
	MathFuncSub     string = "mathSubract"
	MathFuncMul     string = "mathMultiply"
	MathFuncDiv     string = "mathDivide"
	MathFuncMod     string = "mathModulo"
	MathFuncNeg     string = "mathNegate"

	FuncAbs    string = "ABS"
	FuncAcos   string = "ACOS"
	FuncAsin   string = "ASIN"
	FuncAtan   string = "ATAN"
	FuncAtan2  string = "ATAN2"
	FuncCeil   string = "CEIL"
	FuncCos    string = "COS"
	FuncDate   string = "DATE"
	FuncDeg    string = "DEGREES"
	FuncExp    string = "EXP"
	FuncFloor  string = "FLOOR"
	FuncLog    string = "LOG"
	FuncLn     string = "LN"
	FuncPower  string = "POW"
	FuncRad    string = "RADIANS"
	FuncRegexp string = "REGEXP_CONTAINS"
	FuncSin    string = "SIN"
	FuncTan    string = "TAN"
	FuncRound  string = "ROUND"
	FuncSqrt   string = "SQRT"
)

// Parser related constants
const (
	OperatorOr            string = "OR"
	OperatorAnd           string = "AND"
	OperatorNot           string = "NOT"
	OperatorTrue          string = "TRUE"
	OperatorFalse         string = "FALSE"
	OperatorMeta          string = "META"
	OperatorEquals        string = "="
	OperatorEquals2       string = "=="
	OperatorNotEquals     string = "<>"
	OperatorNotEquals2    string = "!="
	OperatorGreaterThan   string = ">"
	OperatorGreaterThanEq string = ">="
	OperatorLessThan      string = "<"
	OperatorLessThanEq    string = "<="
	OperatorExists        string = "EXISTS"
	OperatorMissing       string = "IS MISSING"
	OperatorNotMissing    string = "IS NOT MISSING"
	OperatorNull          string = "IS NULL"
	OperatorNotNull       string = "IS NOT NULL"
)

// Participle parser can cause stack overflow if certain inputs (i.e. a single word regex) is passed in
// This slice allows callers to get a list of valid operators that are used, so they can check whether
// or not a valid expression is valid prior to passing into the FilterExpression Parser
var GojsonsmOperators []string = []string{OperatorOr, OperatorAnd, OperatorNot, OperatorTrue,
	OperatorFalse, OperatorMeta, OperatorEquals, OperatorEquals2, OperatorNotEquals, OperatorNotEquals2, OperatorGreaterThan,
	OperatorGreaterThanEq, OperatorLessThan, OperatorLessThanEq, OperatorExists, OperatorMissing, OperatorNotMissing,
	OperatorNull, OperatorNotNull /* BooleanFuncs*/, FuncRegexp}

// Error constants
var emptyExpression Expression
var ErrorEmptyInput error = fmt.Errorf("Error: Input is empty")
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
var ErrorInvalidTimeFormat error = fmt.Errorf("Invalid given time format")
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

// Function helpers
type checkAndGetKeyFunc func(string) (bool, string)
type funcNameType string
type funcRecursiveIdx int

// Support for pcre's lookahead class of regex
const lookAheadPattern = "\\(\\?\\=.+\\)"
const lookBehindPattern = "\\(\\?\\<.+\\)"
const negLookAheadPattern = "\\(\\?\\!.+\\)"
const negLookBehindPattern = "\\(\\?\\<\\!.+\\)"

var pcreCheckers [4]*regexp.Regexp = [...]*regexp.Regexp{regexp.MustCompile(lookAheadPattern),
	regexp.MustCompile(lookBehindPattern),
	regexp.MustCompile(negLookAheadPattern),
	regexp.MustCompile(negLookBehindPattern)}

// Returns true if the value is to be used for pcre types
func tokenIsPcreValueType(token string) bool {
	for _, pcreChecker := range pcreCheckers {
		if pcreChecker.MatchString(token) {
			return true
		}
	}
	return false
}
