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

var mathDegreeFunc func(float64) float64 = func(rad float64) float64 {
	return rad * 180 / math.Pi
}

var mathRadiansFunc func(float64) float64 = func(deg float64) float64 {
	return deg * math.Pi / 180
}

func FastValMathRound(val FastVal) FastVal {
	if val.IsFloat() {
		originalValue, valid := val.AsFloat()
		if !valid {
			return NewInvalidFastVal()
		}
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
		floatVal, valid := val.AsFloat()
		if !valid {
			return NewInvalidFastVal()
		}
		if val.IsFloat() {
			return NewFloatFastVal(math.Abs(floatVal))
		} else if val.IsInt() {
			return NewIntFastVal(int64(math.Abs(floatVal)))
		}
	}

	return NewInvalidFastVal()
}

type intToIntOp func(int64) int64
type int2ToIntOp func(int64, int64) int64
type floatToFloatOp func(float64) float64
type float2ToFloatOp func(float64, float64) float64

func fastValMathAdd(a, b float64) float64 {
	return a + b
}

func fastValMathSub(a, b float64) float64 {
	return a - b
}

func fastValMathMult(a, b float64) float64 {
	return a * b
}

func fastValMathDiv(a, b float64) float64 {
	return a / b
}

func fastValMathMod(a, b int64) int64 {
	return a % b
}

func fastValNegate(a float64) float64 {
	return -1.0 * a
}

func genericFastValIntOp(val FastVal, op intToIntOp) FastVal {
	intVal, valid := val.AsInt()
	if valid && val.IsNumeric() {
		return NewIntFastVal(op(intVal))
	}

	return NewInvalidFastVal()
}

func genericFastVal2IntsOp(val, val1 FastVal, op int2ToIntOp) FastVal {
	valInt, valid := val.AsInt()
	val1Int, valid2 := val1.AsInt()
	if !val.IsNumeric() || !val1.IsNumeric() || !valid || !valid2 {
		return NewInvalidFastVal()
	}

	return NewIntFastVal(op(valInt, val1Int))
}

func genericFastValFloatOp(val FastVal, op floatToFloatOp) FastVal {
	valFloat, valid := val.AsFloat()
	if valid && val.IsNumeric() {
		return NewFloatFastVal(op(valFloat))
	}

	return NewInvalidFastVal()
}

func genericFastVal2FloatsOp(val, val1 FastVal, op float2ToFloatOp) FastVal {
	valFloat, valid := val.AsFloat()
	val1Float, valid2 := val1.AsFloat()
	if !val.IsNumeric() || !val1.IsNumeric() || !valid || !valid2 {
		return NewInvalidFastVal()
	}

	return NewFloatFastVal(op(valFloat, val1Float))
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

func FastValMathAtan2(val, val1 FastVal) FastVal {
	return genericFastVal2FloatsOp(val, val1, math.Atan2)
}

func FastValMathDegrees(val FastVal) FastVal {
	return genericFastValFloatOp(val, mathDegreeFunc)
}

func FastValMathRadians(val FastVal) FastVal {
	return genericFastValFloatOp(val, mathRadiansFunc)
}

func FastValMathAdd(val, val1 FastVal) FastVal {
	return genericFastVal2FloatsOp(val, val1, fastValMathAdd)
}

func FastValMathSub(val, val1 FastVal) FastVal {
	return genericFastVal2FloatsOp(val, val1, fastValMathSub)
}

func FastValMathMul(val, val1 FastVal) FastVal {
	return genericFastVal2FloatsOp(val, val1, fastValMathMult)
}

func FastValMathDiv(val, val1 FastVal) FastVal {
	return genericFastVal2FloatsOp(val, val1, fastValMathDiv)
}

func FastValMathMod(val, val1 FastVal) FastVal {
	return genericFastVal2IntsOp(val, val1, fastValMathMod)
}

func FastValMathNeg(val FastVal) FastVal {
	return genericFastValFloatOp(val, fastValNegate)
}
