package gojsonsm

// floatRound implements math.Round for Go versions older than
// 1.10 which did not have the function... Wat?
func floatMathRound(val float64) float64 {
	if val < 0 {
		return float64(int(val - 0.5))
	}
	return float64(int(val + 0.5))
}

func FastValMathRound(val FastVal) FastVal {
	if val.IsFloat() {
		originalValue := val.AsFloat()
		roundedValue := floatMathRound(originalValue)
		return NewFloatFastVal(roundedValue)
	} else if val.IsInt() || val.IsUInt() {
		// These values are already rounded, no need for any work
		return val
	}

	return NewInvalidFastVal()
}
