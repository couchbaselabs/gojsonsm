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
const (
	InvalidValue ValueType = iota
	MissingValue
	IntValue
	UintValue
	JsonIntValue
	JsonUintValue
	FloatValue
	JsonFloatValue
	StringValue
	BinStringValue
	JsonStringValue
	RegexValue
	PcreValue
	BinaryValue
	NullValue
	TrueValue
	FalseValue
	ArrayValue
	ObjectValue
	TimeValue
)

type FastVal struct {
	dataType  ValueType
	data      interface{}
	sliceData []byte
	rawData   [8]byte
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

func (val FastVal) AsInt() int64 {
	switch val.dataType {
	case IntValue:
		return val.GetInt()
	case UintValue:
		return int64(val.GetUint())
	case FloatValue:
		return int64(val.GetFloat())
	case JsonIntValue:
		parsedVal, _ := strconv.ParseInt(string(val.sliceData), 10, 64)
		return parsedVal
	case JsonUintValue:
		parsedVal, _ := strconv.ParseUint(string(val.sliceData), 10, 64)
		return int64(parsedVal)
	case JsonFloatValue:
		parsedVal, _ := strconv.ParseFloat(string(val.sliceData), 64)
		return int64(parsedVal)
	case TrueValue:
		return 1
	case FalseValue:
		return 0
	case NullValue:
		return math.MinInt64
	}
	return 0
}

func (val FastVal) AsUint() uint64 {
	switch val.dataType {
	case IntValue:
		return uint64(val.GetInt())
	case UintValue:
		return val.GetUint()
	case FloatValue:
		return uint64(val.GetFloat())
	case JsonIntValue:
		parsedVal, _ := strconv.ParseInt(string(val.sliceData), 10, 64)
		return uint64(parsedVal)
	case JsonUintValue:
		parsedVal, _ := strconv.ParseUint(string(val.sliceData), 10, 64)
		return parsedVal
	case JsonFloatValue:
		parsedVal, _ := strconv.ParseFloat(string(val.sliceData), 64)
		return uint64(parsedVal)
	case TrueValue:
		return 1
	case FalseValue:
		return 0
	case NullValue:
		return math.MaxUint64
	}
	return 0
}

func (val FastVal) AsFloat() float64 {
	switch val.dataType {
	case IntValue:
		return float64(val.GetInt())
	case UintValue:
		return float64(val.GetUint())
	case FloatValue:
		return val.GetFloat()
	case JsonIntValue:
		parsedVal, _ := strconv.ParseInt(string(val.sliceData), 10, 64)
		return float64(parsedVal)
	case JsonUintValue:
		parsedVal, _ := strconv.ParseUint(string(val.sliceData), 10, 64)
		return float64(parsedVal)
	case JsonFloatValue:
		parsedVal, _ := strconv.ParseFloat(string(val.sliceData), 64)
		return parsedVal
	case TrueValue:
		return 1.0
	case FalseValue:
		return 0.0
	case NullValue:
		return math.MaxFloat64
	}
	return 0.0
}

func (val FastVal) AsBoolean() bool {
	return val.AsInt() != 0 && val.AsInt() != math.MinInt64
}

func (val FastVal) AsRegex() FastValRegexIface {
	switch val.dataType {
	case RegexValue:
		return val.data.(*regexp.Regexp)
	case PcreValue:
		return val.data.(PcreWrapperInterface)
	}
	return nil
}

func (val FastVal) AsTime() *time.Time {
	switch val.dataType {
	case TimeValue:
		return val.data.(*time.Time)
	}
	return nil
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

func (val FastVal) ToJsonString() (FastVal, error) {
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
	}

	return val, errors.New("invalid type coercion")
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

func (val FastVal) compareInt(other FastVal) int {
	if other.dataType == FloatValue && other.floatToIntOverflows() {
		return val.compareFloat(other)
	}

	intVal := val.AsInt()
	intOval := other.AsInt()

	if intVal < intOval {
		return -1
	} else if intVal > intOval {
		return 1
	} else {
		return 0
	}
}

func (val FastVal) compareUint(other FastVal) int {
	uintVal := val.AsUint()
	uintOval := other.AsUint()
	if uintVal < uintOval {
		return -1
	} else if uintVal > uintOval {
		return 1
	} else {
		return 0
	}
}

func (val FastVal) compareFloat(other FastVal) int {
	// TODO(brett19): EPISLON probably should be defined better than this
	// possibly even 0 if we want to force exact matching for floats...
	EPSILON := 0.0000001

	floatVal := val.AsFloat()
	floatOval := other.AsFloat()

	// Perform epsilon comparison first
	if math.Abs(floatVal-floatOval) < EPSILON {
		return 0
	}

	// Traditional comparison
	if floatVal < floatOval {
		return -1
	} else if floatVal > floatOval {
		return 1
	} else {
		return 0
	}
}

func (val FastVal) compareBoolean(other FastVal) int {
	// We cheat here and use int comparison mode, since integer conversions
	// of the boolean datatypes are consistent
	return val.compareInt(other)
}

func (val FastVal) compareStrings(other FastVal) int {
	// TODO: Improve string comparisons to avoid casting or converting
	escVal, _ := val.ToJsonString()
	escOval, _ := other.ToJsonString()
	return strings.Compare(string(escVal.sliceData), string(escOval.sliceData))
}

func (val FastVal) compareTime(other FastVal) int {
	thisTime := val.AsTime()
	otherTime := other.AsTime()

	// Consider nil value as the smaller/earlier than non-nil values
	if thisTime == nil && otherTime == nil {
		return 0
	} else if otherTime == nil && thisTime != nil {
		return 1
	} else if otherTime != nil && thisTime == nil {
		return -1
	}

	if thisTime.Equal(*otherTime) {
		return 0
	} else if thisTime.After(*otherTime) {
		return 1
	} else {
		return -1
	}
}

func (val FastVal) compareArray(other FastVal) int {
	// TODO - need a better way but for now treat them the same
	return val.compareObjArrData(other)
}

func (val FastVal) compareObject(other FastVal) int {
	// TODO - need a better way but for now treat them the same
	return val.compareObjArrData(other)
}

func (val FastVal) compareObjArrData(other FastVal) int {
	var same bool
	// Do not use reflect
	switch val.dataType {
	case ArrayValue:
		fallthrough
	case ObjectValue:
		if len(val.sliceData) > len(other.sliceData) {
			return 1
		} else if len(val.sliceData) < len(other.sliceData) {
			return -1
		} else {
			same = true
			for i := range val.sliceData {
				if val.sliceData[i] > other.sliceData[i] {
					return 1
				} else if val.sliceData[i] < other.sliceData[i] {
					return -1
				}
			}
		}
	}
	if same {
		return 0
	} else {
		return -1
	}
}

func (val FastVal) Compare(other FastVal) int {
	switch val.dataType {
	case IntValue:
		return val.compareInt(other)
	case UintValue:
		return val.compareUint(other)
	case FloatValue:
		return val.compareFloat(other)
	case JsonIntValue:
		return val.compareInt(other)
	case JsonUintValue:
		return val.compareUint(other)
	case JsonFloatValue:
		return val.compareFloat(other)
	case StringValue:
		return val.compareStrings(other)
	case BinStringValue:
		return val.compareStrings(other)
	case JsonStringValue:
		return val.compareStrings(other)
	case TrueValue:
		return val.compareBoolean(other)
	case FalseValue:
		return val.compareBoolean(other)
	case TimeValue:
		return val.compareTime(other)
	case ArrayValue:
		return val.compareArray(other)
	case ObjectValue:
		return val.compareObject(other)
	}

	if val.dataType < other.dataType {
		return -1
	} else if val.dataType > other.dataType {
		return 1
	} else {
		return 0
	}
}

func (val FastVal) Equals(other FastVal) bool {
	// TODO: I doubt this logic is correct...
	return val.Compare(other) == 0
}

func (val FastVal) matchStrings(other FastVal) bool {
	escVal, _ := val.ToJsonString()
	return other.AsRegex().Match(escVal.sliceData)
}

func (val FastVal) Matches(other FastVal) bool {
	switch val.dataType {
	case StringValue:
		return val.matchStrings(other)
	case BinStringValue:
		return val.matchStrings(other)
	case JsonStringValue:
		return val.matchStrings(other)
	default:
		return false
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
