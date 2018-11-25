package gojsonsm

import (
	"fmt"
	"time"
)

func FastValDateFunc(val FastVal) FastVal {
	switch val.Type() {
	case TimeValue:
		return val
	case JsonStringValue:
		fallthrough
	case BinStringValue:
		tmpVal, _ := val.ToBinString()
		str := fmt.Sprintf(`%s`, tmpVal.sliceData)
		timeVal, err := time.Parse(time.RFC3339, str)
		if err != nil {
			return NewInvalidFastVal()
		}
		return NewTimeFastVal(&timeVal)
	case StringValue:
		str := val.data.(string)
		timeVal, err := time.Parse(time.RFC3339, str)
		if err != nil {
			return NewInvalidFastVal()
		}
		return NewTimeFastVal(&timeVal)
	}
	return NewInvalidFastVal()
}
