// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type ValueType int

// This must be in comparison precedence order!
// Mirrors as closely as possible with N1QL Collate order
const (
	InvalidValue ValueType = iota
	MissingValue
	NullValue
	FalseValue
	TrueValue
	// Numerics:
	UintValue
	JsonUintValue
	IntValue
	JsonIntValue
	FloatValue
	JsonFloatValue
	// String types
	StringValue
	BinStringValue
	JsonStringValue
	// Time can only be implicitly converted from a specific string form
	TimeValue
	// Array Obj
	ArrayValue
	ObjectValue
	// Binary types
	BinaryValue
	RegexValue
	PcreValue
)

// Implicit Conversion Table
// Keyed by type, and then a list of potentially convertible target datatype with
// "true" for definitely convertible and "false" for potentially convertible
// If a ValueType does not exist in this table, that means there is no possible target conversion
var ImplicitConvTable = map[ValueType]map[ValueType]bool{
	// Numerics
	IntValue: map[ValueType]bool{UintValue: false, JsonUintValue: false, FloatValue: true, JsonFloatValue: true,
		StringValue: true, BinStringValue: true, JsonStringValue: true, NullValue: true, TrueValue: true, FalseValue: true},
	JsonIntValue: map[ValueType]bool{UintValue: false, JsonUintValue: false, FloatValue: true, JsonFloatValue: true,
		StringValue: true, BinStringValue: true, JsonStringValue: true, NullValue: true, TrueValue: true, FalseValue: true},
	UintValue: map[ValueType]bool{IntValue: true, JsonIntValue: true, FloatValue: true, JsonFloatValue: true, StringValue: true,
		BinStringValue: true, JsonStringValue: true, NullValue: true, TrueValue: true, FalseValue: true},
	JsonUintValue: map[ValueType]bool{IntValue: true, JsonIntValue: true, FloatValue: true, JsonFloatValue: true, StringValue: true,
		BinStringValue: true, JsonStringValue: true, NullValue: true, TrueValue: true, FalseValue: true},
	FloatValue: map[ValueType]bool{StringValue: true, BinStringValue: true, JsonStringValue: true, NullValue: true, TrueValue: true,
		FalseValue: true},
	JsonFloatValue: map[ValueType]bool{StringValue: true, BinStringValue: true, JsonStringValue: true, NullValue: true, TrueValue: true,
		FalseValue: true},
	// Non-Numerics
	TrueValue: map[ValueType]bool{IntValue: true, JsonIntValue: true, UintValue: true, JsonUintValue: true, FloatValue: true,
		JsonFloatValue: true, StringValue: true, BinStringValue: true, JsonStringValue: true, NullValue: true, FalseValue: true},
	FalseValue: map[ValueType]bool{IntValue: true, JsonIntValue: true, UintValue: true, JsonUintValue: true, FloatValue: true,
		JsonFloatValue: true, StringValue: true, BinStringValue: true, JsonStringValue: true, NullValue: true, TrueValue: true},
	StringValue: map[ValueType]bool{IntValue: false, JsonIntValue: false, UintValue: false, JsonUintValue: false, FloatValue: false,
		JsonFloatValue: false, NullValue: true, TrueValue: false, FalseValue: false, TimeValue: false},
	BinStringValue: map[ValueType]bool{IntValue: false, JsonIntValue: false, UintValue: false, JsonUintValue: false, FloatValue: false,
		JsonFloatValue: false, NullValue: true, TrueValue: false, FalseValue: false, TimeValue: false},
	JsonStringValue: map[ValueType]bool{IntValue: false, JsonIntValue: false, UintValue: false, JsonUintValue: false, FloatValue: false,
		JsonFloatValue: false, NullValue: true, TrueValue: false, FalseValue: false, TimeValue: false},
}

// When users try to match a string to a bool, the bool is converted to a JSON string
// These are constants
const TrueString = "true"
const FalseString = "false"

var TrueValueBytes = []byte(TrueString)
var FalseValueBytes = []byte(FalseString)

var TrueStringRegex *regexp.Regexp = regexp.MustCompile("^[T|t][R|r][U|u][E|e]$")
var FalseStringRegex *regexp.Regexp = regexp.MustCompile("^[F|f][A|a][L|l][S|s][E|e]$")

var toJsonStringBuffer []byte

type FastVal struct {
	dataType    ValueType
	data        interface{}
	sliceData   []byte
	rawData     [8]byte
	userDefined bool
}

func (val FastVal) String() string {
	switch val.dataType {
	case InvalidValue:
		return "invalid"
	case MissingValue:
		return "missing"
	case IntValue:
		return "(int)" + fmt.Sprintf("%d", val.GetInt())
	case UintValue:
		return "(uint)" + fmt.Sprintf("%d", val.GetUint())
	case FloatValue:
		return "(float)" + fmt.Sprintf("%f", val.GetFloat())
	case JsonIntValue:
		return "(jsonInt)" + string(val.sliceData)
	case JsonUintValue:
		return "(jsonUint)" + string(val.sliceData)
	case JsonFloatValue:
		return "(jsonFloat)" + string(val.sliceData)
	case StringValue:
		return "(string)" + val.data.(string)
	case BinStringValue:
		tmpVal, _ := val.ToBinString()
		return "(binString)" + fmt.Sprintf(`"%s"`, tmpVal.sliceData)
	case JsonStringValue:
		tmpVal, _ := val.ToBinString()
		return "(jsonString)" + fmt.Sprintf(`"%s"`, tmpVal.sliceData)
	case BinaryValue:
		return "(bin)" + fmt.Sprintf(`"%s"`, val.sliceData)
	case NullValue:
		return "null"
	case TrueValue:
		return "true"
	case FalseValue:
		return "false"
	case ArrayValue:
		return "(array)" + string(val.sliceData)
	case ObjectValue:
		return "(object)" + string(val.sliceData)
	case TimeValue:
		return val.GetTime().String()
	case RegexValue:
		return "(regexp)" + val.data.(*regexp.Regexp).String()
	}

	panic(fmt.Sprintf("unexpected data type %v", val.dataType))
}

func (val FastVal) Type() ValueType {
	return val.dataType
}

func (val FastVal) IsMissing() bool {
	return val.dataType == MissingValue
}

func (val FastVal) IsNull() bool {
	return val.dataType == NullValue
}

func (val FastVal) IsBinary() bool {
	return val.dataType == BinaryValue
}

func (val FastVal) IsBoolean() bool {
	return val.dataType == TrueValue ||
		val.dataType == FalseValue
}

func (val FastVal) IsIntegral() bool {
	return val.IsInt() ||
		val.IsUInt()
}

func (val FastVal) IsInt() bool {
	return val.dataType == IntValue ||
		val.dataType == JsonIntValue
}

func (val FastVal) IsUInt() bool {
	return val.dataType == UintValue ||
		val.dataType == JsonUintValue
}

func (val FastVal) IsFloat() bool {
	return val.dataType == FloatValue ||
		val.dataType == JsonFloatValue
}

func (val FastVal) IsNumeric() bool {
	return val.IsInt() ||
		val.IsUInt() ||
		val.IsFloat()
}

func (val FastVal) IsString() bool {
	return val.dataType == StringValue ||
		val.dataType == BinStringValue ||
		val.dataType == JsonStringValue
}

func (val FastVal) IsTime() bool {
	return val.dataType == TimeValue
}

func (val FastVal) GetInt() int64 {
	return *(*int64)(unsafe.Pointer(&val.rawData))
}

func (val FastVal) GetUint() uint64 {
	return *(*uint64)(unsafe.Pointer(&val.rawData))
}

func (val FastVal) GetFloat() float64 {
	return *(*float64)(unsafe.Pointer(&val.rawData))
}

func (val FastVal) GetTime() *time.Time {
	return val.data.(*time.Time)
}

func (val FastVal) AsInt() (int64, bool) {
	switch val.dataType {
	case IntValue:
		return val.GetInt(), true
	case UintValue:
		uintVal := val.GetUint()
		return int64(uintVal), uintVal <= math.MaxInt64
	case FloatValue:
		return int64(val.GetFloat()), false
	case BinStringValue:
		fallthrough
	case JsonStringValue:
		fallthrough
	case JsonIntValue:
		parsedVal, err := strconv.ParseInt(string(val.sliceData), 10, 64)
		return parsedVal, err == nil
	case JsonUintValue:
		parsedVal, err := strconv.ParseUint(string(val.sliceData), 10, 64)
		if err == nil && parsedVal > math.MaxInt64 {
			return 0, false
		} else {
			return int64(parsedVal), err == nil
		}
	case JsonFloatValue:
		parsedVal, _ := strconv.ParseFloat(string(val.sliceData), 64)
		return int64(parsedVal), false
	case TrueValue:
		return 1, true
	case FalseValue:
		return 0, true
	}
	return 0, false
}

func (val FastVal) AsUint() (uint64, bool) {
	switch val.dataType {
	case IntValue:
		intVal := val.GetInt()
		if intVal > 0 {
			return uint64(intVal), true
		} else {
			return 0, false
		}
	case UintValue:
		return val.GetUint(), true
	case FloatValue:
		return uint64(val.GetFloat()), false
	case JsonIntValue:
		parsedVal, err := strconv.ParseInt(string(val.sliceData), 10, 64)
		if err == nil && parsedVal > 0 {
			return uint64(parsedVal), true
		} else {
			return 0, false
		}
	case BinStringValue:
		fallthrough
	case JsonStringValue:
		tmpVal, _ := val.ToBinString()
		parsedVal, err := strconv.ParseUint(string(tmpVal.sliceData), 10, 64)
		return parsedVal, err == nil
	case JsonUintValue:
		parsedVal, err := strconv.ParseUint(string(val.sliceData), 10, 64)
		return parsedVal, err == nil
	case JsonFloatValue:
		parsedVal, err := strconv.ParseFloat(string(val.sliceData), 64)
		return uint64(parsedVal), err == nil
	case TrueValue:
		return 1, true
	case FalseValue:
		return 0, true
	}
	return 0, false
}

func (val FastVal) AsFloat() (float64, bool) {
	switch val.dataType {
	case IntValue:
		return float64(val.GetInt()), true
	case UintValue:
		return float64(val.GetUint()), true
	case FloatValue:
		return val.GetFloat(), true
	case JsonIntValue:
		parsedVal, err := strconv.ParseInt(string(val.sliceData), 10, 64)
		return float64(parsedVal), err == nil
	case JsonUintValue:
		parsedVal, err := strconv.ParseUint(string(val.sliceData), 10, 64)
		return float64(parsedVal), err == nil
	case JsonStringValue:
		fallthrough
	case JsonFloatValue:
		parsedVal, err := strconv.ParseFloat(string(val.sliceData), 64)
		return parsedVal, err == nil
	case TrueValue:
		return 1.0, true
	case FalseValue:
		return 0.0, true
	}
	return 0.0, false
}

func (val FastVal) AsBoolean() (bool, bool) {
	switch val.dataType {
	case JsonStringValue:
		if TrueStringRegex.Match(val.sliceData) {
			return true, true
		} else if FalseStringRegex.Match(val.sliceData) {
			return false, true
		} else {
			return false, false
		}
	case IntValue:
		return val.GetInt() != 0, true
	case UintValue:
		return val.GetUint() != 0, true
	case FloatValue:
		return val.GetFloat() != 0.0, true
	case JsonIntValue:
		parsedVal, err := strconv.ParseInt(string(val.sliceData), 10, 64)
		return parsedVal != 0, err == nil
	case JsonUintValue:
		parsedVal, err := strconv.ParseUint(string(val.sliceData), 10, 64)
		return parsedVal != 0, err == nil
	case JsonFloatValue:
		parsedVal, err := strconv.ParseFloat(string(val.sliceData), 64)
		return parsedVal != 0.0, err == nil
	case TrueValue:
		return true, true
	case FalseValue:
		return false, true
	default:
		// Undefined
		return true, false
	}
}

func (val FastVal) AsString() (string, bool) {
	switch val.dataType {
	case StringValue:
		return val.data.(string), true
	case JsonStringValue:
		fallthrough
	case BinStringValue:
		tmpVal, _ := val.ToBinString()
		return string(tmpVal.sliceData), true
	case IntValue:
		return fmt.Sprintf("%d", val.GetInt()), true
	case UintValue:
		return fmt.Sprintf("%d", val.GetUint()), true
	case FloatValue:
		return fmt.Sprintf("%f", val.GetFloat()), true
	case JsonIntValue:
		return string(val.sliceData), true
	case JsonUintValue:
		return string(val.sliceData), true
	case JsonFloatValue:
		return string(val.sliceData), true
	case TrueValue:
		return TrueString, true
	case FalseValue:
		return FalseString, true
	}
	return "", false
}

func (val FastVal) AsRegex() (FastValRegexIface, bool) {
	switch val.dataType {
	case RegexValue:
		return val.data.(*regexp.Regexp), true
	case PcreValue:
		return val.data.(PcreWrapperInterface), true
	}
	return nil, false
}

func (val FastVal) AsTime() (*time.Time, bool) {
	switch val.dataType {
	case TimeValue:
		return val.data.(*time.Time), true
	case StringValue:
		fallthrough
	case JsonStringValue:
		fallthrough
	case BinStringValue:
		timeFastVal, err := GetNewTimeFastVal(string(val.sliceData))
		if err == nil {
			return timeFastVal.data.(*time.Time), true
		}
	}
	return nil, false
}

func (val FastVal) ToBinString() (FastVal, error) {
	switch val.dataType {
	case StringValue:
		return NewBinStringFastVal([]byte(val.data.(string))), nil
	case BinStringValue:
		return val, nil
	case JsonStringValue:
		// TODO: MUST DO - Unescape!
		return val, nil
	}

	return val, errors.New("invalid type coercion")
}

// The following reuse an internal buffer so this should be hidden from outside callers
// Internally, this must be called only once per comparison, and should be used for implicit comversion
// (i.e. no double implicit conversion to string from the following 3 types)
func (val FastVal) toJsonStringInternal() (FastVal, error) {
	val, err := val.ToJsonString()

	if err != nil {
		switch val.dataType {
		case UintValue:
			toJsonStringBuffer = toJsonStringBuffer[:0]
			toJsonStringBuffer = strconv.AppendUint(toJsonStringBuffer, val.GetUint(), 10)
			return NewJsonStringFastVal(toJsonStringBuffer), nil
		case IntValue:
			toJsonStringBuffer = toJsonStringBuffer[:0]
			toJsonStringBuffer = strconv.AppendInt(toJsonStringBuffer, val.GetInt(), 10)
			return NewJsonStringFastVal(toJsonStringBuffer), nil
		case FloatValue:
			toJsonStringBuffer = toJsonStringBuffer[:0]
			toJsonStringBuffer = strconv.AppendFloat(toJsonStringBuffer, val.GetFloat(), 'E', -1, 64)
			return NewJsonStringFastVal(toJsonStringBuffer), nil
		}
	}

	return val, err
}

func (val FastVal) ToJsonString() (FastVal, error) {
	invalidErr := errors.New("invalid type coercion")
	switch val.dataType {
	case StringValue:
		// TODO: Improve AsJsonString allocations
		quotedBytes := strconv.AppendQuote(nil, val.data.(string))
		return NewJsonStringFastVal(quotedBytes[1 : len(quotedBytes)-1]), nil
	case BinStringValue:
		// TODO: Improve AsJsonString allocaitons
		quotedBytes := strconv.AppendQuote(nil, string(val.sliceData))
		return NewJsonStringFastVal(quotedBytes[1 : len(quotedBytes)-1]), nil
	case JsonStringValue:
		return val, nil
	case TrueValue:
		return NewJsonStringFastVal(TrueValueBytes), nil
	case FalseValue:
		return NewJsonStringFastVal(FalseValueBytes), nil
	case NullValue:
		return NewInvalidFastVal(), invalidErr
	}
	return val, invalidErr
}

func (val FastVal) floatToIntOverflows() bool {
	floatVal := val.GetFloat()

	// Instead of using math constants that could potentially lead to rounding errors,
	// force a float-to-float comparison here
	if !(floatVal >= math.MinInt64 && floatVal <= math.MaxInt64) {
		return true
	} else {
		return false
	}
}

func (val FastVal) compareNull(other FastVal) (int, bool) {
	if val.IsNull() && other.IsNull() {
		return 0, true
	} else if val.IsNull() && !other.IsNull() {
		return -1, true
	} else if !val.IsNull() && other.IsNull() {
		return 1, true
	} else {
		// Shouldn't be possible
		return 0, false
	}
}

func (val FastVal) compareInt(other FastVal) (int, bool) {
	if other.dataType == FloatValue && other.floatToIntOverflows() {
		return val.compareFloat(other)
	}

	intVal, valid := val.AsInt()
	intOval, valid2 := other.AsInt()

	if intVal < intOval {
		return -1, valid && valid2
	} else if intVal > intOval {
		return 1, valid && valid2
	} else {
		return 0, valid && valid2
	}
}

func (val FastVal) compareUint(other FastVal) (int, bool) {
	uintVal, valid := val.AsUint()
	uintOval, valid2 := other.AsUint()

	if uintVal < uintOval {
		return -1, valid && valid2
	} else if uintVal > uintOval {
		return 1, valid && valid2
	} else {
		return 0, valid && valid2
	}
}

func (val FastVal) compareFloat(other FastVal) (int, bool) {
	// TODO(brett19): EPISLON probably should be defined better than this
	// possibly even 0 if we want to force exact matching for floats...
	EPSILON := 0.0000001

	floatVal, valid := val.AsFloat()
	floatOval, valid2 := other.AsFloat()

	if math.IsNaN(floatVal) || math.IsNaN(floatOval) {
		// Comparing Not-A-Number
		// Documentation wise - they should be aware that NaN ops are undefined
		// In the meantime - because we have to return something, just let imaginary numbers be < real numbers
		if math.IsNaN(floatVal) && math.IsNaN(floatOval) {
			return 0, false
		} else if math.IsNaN(floatVal) && !math.IsNaN(floatOval) {
			return -1, false
		} else if !math.IsNaN(floatVal) && math.IsNaN(floatOval) {
			return 1, false
		}
	}

	// Perform epsilon comparison first
	if math.Abs(floatVal-floatOval) < EPSILON {
		return 0, valid && valid2
	}

	// Traditional comparison
	if floatVal < floatOval {
		return -1, valid && valid2
	} else if floatVal > floatOval {
		return 1, valid && valid2
	} else {
		return 0, valid && valid2
	}
}

func (val FastVal) compareBoolean(other FastVal) (int, bool) {
	valBool, valid := val.AsBoolean()
	otherBool, valid2 := other.AsBoolean()
	if !valid || !valid2 {
		return 0, false
	}

	if valBool == otherBool {
		return 0, true
	} else if valBool && !otherBool {
		return 1, true
	} else {
		return -1, true
	}
}

func (val FastVal) compareStrings(other FastVal) (int, bool) {
	if other.IsString() || other.IsNumeric() {
		escVal, err := val.toJsonStringInternal()
		escOval, err1 := other.toJsonStringInternal()

		result := strings.Compare(string(escVal.sliceData), string(escOval.sliceData))
		return result, err == nil && err1 == nil
	} else if other.userDefined {
		// User defined means that this val should try to implicit convert to the other type
		switch other.dataType {
		case TrueValue:
			fallthrough
		case FalseValue:
			if TrueStringRegex.Match(val.sliceData) || FalseStringRegex.Match(val.sliceData) {
				return val.compareBoolean(other)
			}
		case TimeValue:
			_, err := GetNewTimeFastVal(string(val.sliceData))
			if err == nil {
				return val.compareTime(other)
			}
		}
	}
	return 0, false
}

func (val FastVal) compareTime(other FastVal) (int, bool) {
	thisTime, valid := val.AsTime()
	otherTime, valid2 := other.AsTime()

	if thisTime == nil || otherTime == nil {
		return 0, false
	}

	if thisTime.Equal(*otherTime) {
		return 0, valid && valid2
	} else if thisTime.After(*otherTime) {
		return 1, valid && valid2
	} else {
		return -1, valid && valid2
	}
}

func (val FastVal) compareArray(other FastVal) (int, bool) {
	// TODO - need a better way but for now treat them the same
	return val.compareObjArrData(other)
}

func (val FastVal) compareObject(other FastVal) (int, bool) {
	// TODO - need a better way but for now treat them the same
	return val.compareObjArrData(other)
}

func (val FastVal) compareObjArrData(other FastVal) (int, bool) {
	// Do not use reflect
	switch val.dataType {
	case ArrayValue:
		fallthrough
	case ObjectValue:
		if len(val.sliceData) > len(other.sliceData) {
			return 1, true
		} else if len(val.sliceData) < len(other.sliceData) {
			return -1, true
		} else {
			for i := range val.sliceData {
				if val.sliceData[i] > other.sliceData[i] {
					return 1, true
				} else if val.sliceData[i] < other.sliceData[i] {
					return -1, true
				}
			}
			return 0, true
		}
	default:
		return -1, false
	}
}

// This is really using other as the baseline for calling compare,
// and then reversing the result
// This is so that comparisons between different data types are bidirectionally consistent
func (val FastVal) reverseCompare(other FastVal) (int, bool) {
	result, valid := other.compareInternal(val)
	return result * -1, valid
}

// Collate is used when valid comparisons cannot be done
func (val FastVal) Collate(other FastVal) (int, bool) {
	if val.dataType == other.dataType {
		return 0, false
	} else if val.dataType < other.dataType {
		return -1, false
	} else {
		return 1, false
	}
}

func (val FastVal) Compare(other FastVal) (int, bool) {
	if val.userDefined || other.userDefined {
		return val.compareUserDefined(other)
	} else if val.isSameDataTypeAs(other) {
		return val.compareInternal(other)
	} else {
		// Two pass compare - try to cast to the more restrictive type first
		if val.dataType < other.dataType {
			compatible := val.checkCompatibility(other)
			if !compatible {
				reverseCompatible := other.checkCompatibility(val)
				if !reverseCompatible {
					return val.Collate(other)
				} else {
					return val.reverseCompare(other)
				}
			} else {
				return val.compareInternal(other)
			}
		} else {
			reverseCompatible := other.checkCompatibility(val)
			if !reverseCompatible {
				compatible := val.checkCompatibility(other)
				if !compatible {
					return val.Collate(other)
				} else {
					return val.compareInternal(other)
				}
			} else {
				return val.reverseCompare(other)
			}
		}
	}
}

// Shouldn't allow users to have both defined
func (val FastVal) compareUserDefined(other FastVal) (int, bool) {
	bothAreNumeric := val.IsNumeric() && other.IsNumeric()

	if val.userDefined {
		compatible := val.checkCompatibility(other)
		if !compatible {
			if bothAreNumeric {
				return val.reverseCompare(other)
			}
			return val.Collate(other)
		}

		// Force the other to implicitly cast to val if needed
		return val.compareInternal(other)
	} else {
		compatible := other.checkCompatibility(val)
		if !compatible {
			if bothAreNumeric {
				return val.reverseCompare(other)
			}
			return val.Collate(other)
		}

		// Force val to implicitly cast to other if needed
		return val.reverseCompare(other)
	}
}

// Prereq: This call should only be called when "other" is a compatible type
func (val FastVal) compareNumerics(other FastVal) (int, bool) {
	switch val.dataType {
	case JsonIntValue:
		fallthrough
	case IntValue:
		switch other.dataType {
		case FloatValue:
			fallthrough
		case JsonFloatValue:
			return val.compareFloat(other)
		case UintValue:
			fallthrough
		case JsonUintValue:
			intVal, _ := val.AsInt()
			if intVal >= 0 {
				return val.compareUint(other)
			} else {
				return val.compareInt(other)
			}
		default:
			return val.compareInt(other)
		}
	case JsonUintValue:
		fallthrough
	case UintValue:
		switch other.dataType {
		case IntValue:
			fallthrough
		case JsonIntValue:
			intVal, _ := val.AsInt()
			if intVal >= 0 {
				return val.compareUint(other)
			} else {
				return val.compareInt(other)
			}
		case FloatValue:
			fallthrough
		case JsonFloatValue:
			return val.compareFloat(other)
		default:
			return val.compareUint(other)
		}
	case FloatValue:
		fallthrough
	case JsonFloatValue:
		return val.compareFloat(other)
	default:
		panic("Invalid call into compareNumerics")
	}
}

func (val FastVal) isSameDataTypeAs(other FastVal) bool {
	if val.dataType == other.dataType {
		return true
	} else if val.IsString() && other.IsString() {
		return true
	} else {
		return false
	}
}

// Check if other can be implicitly converted to val type
func (val FastVal) checkCompatibility(other FastVal) bool {
	if val.isSameDataTypeAs(other) {
		return true
	} else if val.IsNull() {
		// Anything can be compared with null
		return true
	}

	compatibleTypes, ok := ImplicitConvTable[other.dataType]
	if !ok {
		// other's datatype cannot be casted to anything else
		return false
	}

	fullyCompatible, ok := compatibleTypes[val.dataType]
	if !ok {
		// Val's type is not something that is convertible from other
		return false
	}

	if fullyCompatible {
		return true
	} else {
		// Need to check to see if it is really convertible
		var compatible bool
		switch val.dataType {
		case TimeValue:
			_, compatible = other.AsTime()
		case UintValue:
			fallthrough
		case JsonUintValue:
			_, compatible = other.AsUint()
		case IntValue:
			fallthrough
		case JsonIntValue:
			_, compatible = other.AsInt()
		case FloatValue:
			fallthrough
		case JsonFloatValue:
			_, compatible = other.AsFloat()
		case TrueValue:
			fallthrough
		case FalseValue:
			_, compatible = other.AsBoolean()
		case StringValue:
		case BinStringValue:
		case JsonStringValue:
			_, compatible = other.AsString()
		}
		return compatible
	}
}

// Returns compared val and boolean indicating if the comparison is valid
func (val FastVal) compareInternal(other FastVal) (int, bool) {
	switch val.dataType {
	case IntValue:
		fallthrough
	case UintValue:
		fallthrough
	case FloatValue:
		fallthrough
	case JsonIntValue:
		fallthrough
	case JsonUintValue:
		fallthrough
	case JsonFloatValue:
		return val.compareNumerics(other)
	case StringValue:
		fallthrough
	case BinStringValue:
		fallthrough
	case JsonStringValue:
		return val.compareStrings(other)
	case TrueValue:
		fallthrough
	case FalseValue:
		return val.compareBoolean(other)
	case TimeValue:
		return val.compareTime(other)
	case ArrayValue:
		return val.compareArray(other)
	case ObjectValue:
		return val.compareObject(other)
	case NullValue:
		return val.compareNull(other)
	}

	return val.Collate(other)
}

func (val FastVal) Equals(other FastVal) (bool, bool) {
	result, valid := val.Compare(other)
	equals := result == 0
	if !valid {
		equals = false
	}
	return equals, valid
}

func (val FastVal) matchStrings(other FastVal) (bool, bool) {
	escVal, err := val.toJsonStringInternal()
	if err != nil {
		return false, false
	}

	regex, valid := other.AsRegex()
	if !valid {
		return false, valid
	} else {
		return regex.Match(escVal.sliceData), true
	}
}

func (val FastVal) Matches(other FastVal) (bool, bool) {
	switch val.dataType {
	case StringValue:
		return val.matchStrings(other)
	case BinStringValue:
		return val.matchStrings(other)
	case JsonStringValue:
		return val.matchStrings(other)
	default:
		return false, false
	}
}

func NewFastVal(val interface{}) FastVal {
	switch val := val.(type) {
	case int:
		return NewIntFastVal(int64(val))
	case int8:
		return NewIntFastVal(int64(val))
	case int16:
		return NewIntFastVal(int64(val))
	case int32:
		return NewIntFastVal(int64(val))
	case int64:
		return NewIntFastVal(int64(val))
	case uint:
		return NewUintFastVal(uint64(val))
	case uint8:
		return NewUintFastVal(uint64(val))
	case uint16:
		return NewUintFastVal(uint64(val))
	case uint32:
		return NewUintFastVal(uint64(val))
	case uint64:
		return NewUintFastVal(val)
	case float32:
		return NewFloatFastVal(float64(val))
	case float64:
		return NewFloatFastVal(val)
	case bool:
		return NewBoolFastVal(val)
	case string:
		return NewStringFastVal(val)
	case []byte:
		return NewBinaryFastVal(val)
	case *regexp.Regexp:
		return NewRegexpFastVal(val)
	case PcreWrapperInterface:
		return NewPcreFastVal(val)
	case *time.Time:
		return NewTimeFastVal(val)
	case nil:
		return NewNullFastVal()
	}

	return FastVal{
		dataType: InvalidValue,
	}
}

func NewInvalidFastVal() FastVal {
	return FastVal{
		dataType: InvalidValue,
	}
}

func NewMissingFastVal() FastVal {
	return FastVal{
		dataType: MissingValue,
	}
}

func NewNullFastVal() FastVal {
	return FastVal{
		dataType: NullValue,
	}
}

func NewBoolFastVal(value bool) FastVal {
	if value {
		return FastVal{
			dataType: TrueValue,
		}
	} else {
		return FastVal{
			dataType: FalseValue,
		}
	}
}

func NewIntFastVal(value int64) FastVal {
	val := FastVal{
		dataType: IntValue,
	}
	*(*int64)(unsafe.Pointer(&val.rawData)) = value
	return val
}

func NewUintFastVal(value uint64) FastVal {
	val := FastVal{
		dataType: UintValue,
	}
	*(*uint64)(unsafe.Pointer(&val.rawData)) = value
	return val
}

func NewFloatFastVal(value float64) FastVal {
	val := FastVal{
		dataType: FloatValue,
	}
	*(*float64)(unsafe.Pointer(&val.rawData)) = value
	return val
}

func NewBinStringFastVal(value []byte) FastVal {
	return FastVal{
		dataType:  BinStringValue,
		sliceData: value,
	}
}

func NewStringFastVal(value string) FastVal {
	return FastVal{
		dataType: StringValue,
		data:     value,
	}
}

func NewBinaryFastVal(value []byte) FastVal {
	return FastVal{
		dataType:  BinaryValue,
		sliceData: value,
	}
}

func NewJsonStringFastVal(value []byte) FastVal {
	return FastVal{
		dataType:  JsonStringValue,
		sliceData: value,
	}
}

func NewJsonFloatFastVal(value []byte) FastVal {
	return FastVal{
		dataType:  JsonFloatValue,
		sliceData: value,
	}
}

func NewJsonIntFastVal(value []byte) FastVal {
	return FastVal{
		dataType:  JsonIntValue,
		sliceData: value,
	}
}

func NewJsonUintFastVal(value []byte) FastVal {
	return FastVal{
		dataType:  JsonUintValue,
		sliceData: value,
	}
}

func NewRegexpFastVal(value *regexp.Regexp) FastVal {
	val := FastVal{
		dataType: RegexValue,
		data:     value,
	}
	return val
}

func NewPcreFastVal(value PcreWrapperInterface) FastVal {
	val := FastVal{
		dataType: PcreValue,
		data:     value,
	}
	return val
}

func NewTimeFastVal(value *time.Time) FastVal {
	val := FastVal{
		dataType: TimeValue,
		data:     value,
	}
	return val
}

func NewObjectFastVal(value []byte) FastVal {
	val := FastVal{
		dataType:  ObjectValue,
		sliceData: value,
	}
	return val
}

func NewArrayFastVal(value []byte) FastVal {
	val := FastVal{
		dataType:  ArrayValue,
		sliceData: value,
	}
	return val
}

type FastValRegexIface interface {
	Match(b []byte) bool
}
