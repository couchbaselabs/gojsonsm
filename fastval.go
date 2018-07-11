// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	BinaryValue
	NullValue
	TrueValue
	FalseValue
	ArrayValue
	ObjectValue
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
		return fmt.Sprintf("%d", val.GetInt())
	case UintValue:
		return fmt.Sprintf("%d", val.GetUint())
	case JsonIntValue:
		return string(val.sliceData)
	case JsonUintValue:
		return string(val.sliceData)
	case FloatValue:
		return fmt.Sprintf("%f", val.GetFloat())
	case JsonFloatValue:
		return string(val.sliceData)
	case StringValue:
		return val.data.(string)
	case BinStringValue:
		tmpVal, _ := val.AsBinString()
		return fmt.Sprintf(`"%s"`, tmpVal.sliceData)
	case JsonStringValue:
		tmpVal, _ := val.AsBinString()
		return fmt.Sprintf(`"%s"`, tmpVal.sliceData)
	case BinaryValue:
		return fmt.Sprintf(`"%s"`, val.sliceData)
	case NullValue:
		return "null"
	case TrueValue:
		return "true"
	case FalseValue:
		return "false"
	case ArrayValue:
		// TODO: Implement array value stringification
		return "??ARRAY??"
	case ObjectValue:
		// TODO: Implement array value stringification
		return "??OBJECT??"
	}

	panic("unexpected data type")
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
	return val.dataType == IntValue ||
		val.dataType == UintValue
}

func (val FastVal) IsUnsigned() bool {
	return val.dataType == UintValue
}

func (val FastVal) IsNumeric() bool {
	return val.dataType == IntValue ||
		val.dataType == UintValue ||
		val.dataType == FloatValue
}

func (val FastVal) IsStringLike() bool {
	return val.dataType == StringValue ||
		val.dataType == BinStringValue ||
		val.dataType == JsonStringValue
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

func (val FastVal) AsBinString() (FastVal, error) {
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

func (val FastVal) AsJsonString() (FastVal, error) {
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

func (val FastVal) compareStrings(other FastVal) int {
	// TODO: Improve string comparisons to avoid casting or converting
	escVal, _ := val.AsJsonString()
	escOval, _ := other.AsJsonString()
	return strings.Compare(string(escVal.sliceData), string(escOval.sliceData))
}

func (val FastVal) Compare(other FastVal) int {
	switch val.dataType {
	case StringValue:
		return val.compareStrings(other)
	case BinStringValue:
		return val.compareStrings(other)
	case JsonStringValue:
		return val.compareStrings(other)
	case TrueValue:
		switch other.Type() {
		// Json parser right now parses it as a json string value
		case JsonStringValue:
			// TODO - fix jsonscanner so it will be a TrueValue too
			if other.String() == "\"true\"" {
				return 0
			} else {
				return 1
			}
		}
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

func NewFastVal(val interface{}) FastVal {
	switch val := val.(type) {
	case int8:
		return NewIntFastVal(int64(val))
	case int16:
		return NewIntFastVal(int64(val))
	case int32:
		return NewIntFastVal(int64(val))
	case int64:
		return NewIntFastVal(int64(val))
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
	case nil:
		return NewNullFastVal()
	}

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
