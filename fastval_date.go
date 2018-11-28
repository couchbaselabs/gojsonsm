package gojsonsm

import (
	"fmt"
	"regexp"
	"time"
)

var iso8601Year *regexp.Regexp = regexp.MustCompile(`^(19|20)\d\d$`)
var iso8601YearAndMonth *regexp.Regexp = regexp.MustCompile(`^(19|20)\d\d[- /.](0[1-9]|1[012])$`)
var iso8601CompleteDate *regexp.Regexp = regexp.MustCompile(`^(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])$`)

func validTimeChecker(s string) bool {
	_, err := time.Parse(time.RFC3339, s)
	return err == nil || iso8601Year.MatchString(s) || iso8601YearAndMonth.MatchString(s) || iso8601CompleteDate.MatchString(s)
}

func isoToRfc(str string) string {
	if iso8601Year.MatchString(str) {
		str = fmt.Sprintf(`%s-01-01T00:00:00Z`, str)
	} else if iso8601YearAndMonth.MatchString(str) {
		str = fmt.Sprintf(`%s-01T00:00:00Z`, str)
	} else if iso8601CompleteDate.MatchString(str) {
		str = fmt.Sprintf(`%sT00:00:00Z`, str)
	}
	return str
}

func FastValDateFunc(val FastVal) FastVal {
	switch val.Type() {
	case TimeValue:
		return val
	case JsonStringValue:
		fallthrough
	case BinStringValue:
		binVal, _ := val.ToBinString()
		var str string
		if iso8601Year.Match(binVal.sliceData) {
			str = fmt.Sprintf(`%s-01-01T00:00:00Z`, binVal.sliceData)
		} else if iso8601YearAndMonth.Match(binVal.sliceData) {
			str = fmt.Sprintf(`%s-01T00:00:00Z`, binVal.sliceData)
		} else if iso8601CompleteDate.Match(binVal.sliceData) {
			str = fmt.Sprintf(`%sT00:00:00Z`, binVal.sliceData)
		} else {
			str = fmt.Sprintf(`%s`, binVal.sliceData)
		}
		timeVal, err := time.Parse(time.RFC3339, str)
		if err != nil {
			return NewInvalidFastVal()
		}
		return NewTimeFastVal(&timeVal)
	case StringValue:
		str := isoToRfc(val.data.(string))
		timeVal, err := time.Parse(time.RFC3339, str)
		if err != nil {
			return NewInvalidFastVal()
		}
		return NewTimeFastVal(&timeVal)
	}
	return NewInvalidFastVal()
}

func GetNewTimeFastVal(input string) (FastVal, error) {
	str := isoToRfc(input)
	if timeVal, err := time.Parse(time.RFC3339, str); err == nil {
		return NewFastVal(&timeVal), nil
	} else {
		return NewInvalidFastVal(), err
	}
}
