package gojsonsm

import (
	"math"
)

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

func FastValMathAbs(val FastVal) FastVal {
	if val.IsUInt() {
		// Not gonna do abs on an uint
		return val
	} else {
		if val.IsFloat() {
			return NewFloatFastVal(math.Abs(val.AsFloat()))
		} else if val.IsInt() {
			return NewIntFastVal(int64(math.Abs(val.AsFloat())))
		}
	}

	return NewInvalidFastVal()
}

type floatToFloatOp func(float64) float64
type float2ToFloatOp func(float64, float64) float64

func genericFastValFloatOp(val FastVal, op floatToFloatOp) FastVal {
	if val.IsNumeric() {
		return NewFloatFastVal(op(val.AsFloat()))
	}

	return NewInvalidFastVal()
}

func genericFastVal2FloatsOp(val, val1 FastVal, op float2ToFloatOp) FastVal {
	if !val.IsNumeric() || !val1.IsNumeric() {
		return NewInvalidFastVal()
	}

	return NewFloatFastVal(op(val.AsFloat(), val1.AsFloat()))
}

func FastValMathSqrt(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Sqrt)
}

func FastValMathAcos(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Acos)
}

func FastValMathAsin(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Asin)
}

func FastValMathAtan(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Atan)
}

func FastValMathCos(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Cos)
}

func FastValMathSin(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Sin)
}

func FastValMathTan(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Tan)
}

func FastValMathExp(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Exp)
}

func FastValMathLn(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Log)
}

func FastValMathLog(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Log10)
}

func FastValMathCeil(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Ceil)
}

func FastValMathFloor(val FastVal) FastVal {
	return genericFastValFloatOp(val, math.Floor)
}

func FastValMathPow(val, val1 FastVal) FastVal {
	return genericFastVal2FloatsOp(val, val1, math.Pow)
}
