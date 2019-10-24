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
type uint2ToUintOp func(uint64, uint64) uint64
type floatToFloatOp func(float64) float64
type float2ToFloatOp func(float64, float64) float64

func fastValMathAdd(a, b float64) float64 {
	return a + b
}

func fastValMathAddInt(a, b int64) int64 {
	return a + b
}

func fastValMathAddUint(a, b uint64) uint64 {
	return a + b
}

func fastValMathSub(a, b float64) float64 {
	return a - b
}

func fastValMathSubInt(a, b int64) int64 {
	return a - b
}

func fastValMathSubUint(a, b uint64) uint64 {
	return a - b
}

func fastValMathMult(a, b float64) float64 {
	return a * b
}

func fastValMathMultInt(a, b int64) int64 {
	return a * b
}

func fastValMathMultUint(a, b uint64) uint64 {
	return a * b
}

func fastValMathDiv(a, b float64) float64 {
	return a / b
}

func fastValMathDivInt(a, b int64) int64 {
	return a / b
}

func fastValMathDivUint(a, b uint64) uint64 {
	return a / b
}

func fastValMathMod(a, b int64) int64 {
	return a % b
}

func fastValMathModUint(a, b uint64) uint64 {
	return a % b
}

func fastValNegate(a float64) float64 {
	return -1.0 * a
}

func fastValNegateInt(a int64) int64 {
	return -1 * a
}

func genericFastVal2IntsOp(val, val1 FastVal, op int2ToIntOp) FastVal {
	valInt, valid := val.AsInt()
	val1Int, valid2 := val1.AsInt()
	if !val.IsNumeric() || !val1.IsNumeric() || !valid || !valid2 {
		return NewInvalidFastVal()
	}

	return NewIntFastVal(op(valInt, val1Int))
}

func genericFastVal2UintsOp(val, val1 FastVal, op uint2ToUintOp) FastVal {
	valUint, valid := val.AsUint()
	val1Uint, valid2 := val1.AsUint()
	if !val.IsNumeric() || !val1.IsNumeric() || !valid || !valid2 {
		return NewInvalidFastVal()
	}

	return NewUintFastVal(op(valUint, val1Uint))
}

func genericFastValIntOp(val FastVal, op intToIntOp) FastVal {
	intVal, valid := val.AsInt()
	if valid && val.IsNumeric() {
		return NewIntFastVal(op(intVal))
	}

	return NewInvalidFastVal()
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

func fastValMathAddMultSharedLogic(val, val1 FastVal, intOp int2ToIntOp, uintOp uint2ToUintOp, floatOp float2ToFloatOp) FastVal {
	switch val.dataType {
	case UintValue:
		fallthrough
	case JsonUintValue:
		switch val1.dataType {
		case UintValue:
			fallthrough
		case JsonUintValue:
			return genericFastVal2UintsOp(val, val1, uintOp)
		case IntValue:
			fallthrough
		case JsonIntValue:
			intCheck, valid := val1.AsInt()
			if !valid {
				return NewInvalidFastVal()
			}
			if intCheck >= 0 {
				return genericFastVal2UintsOp(val, val1, uintOp)
			} else {
				uintCheck, valid1 := val.AsUint()
				if !valid1 || uintCheck > math.MaxInt64 {
					return NewInvalidFastVal()
				}
				return genericFastVal2IntsOp(val, val1, intOp)
			}
		case FloatValue:
			fallthrough
		case JsonFloatValue:
			return genericFastVal2FloatsOp(val, val1, floatOp)
		default:
			return NewInvalidFastVal()
		}
	case IntValue:
		fallthrough
	case JsonIntValue:
		switch val1.dataType {
		case UintValue:
			fallthrough
		case JsonUintValue:
			intCheck, valid := val.AsInt()
			if !valid {
				return NewInvalidFastVal()
			}
			if intCheck >= 0 {
				return genericFastVal2UintsOp(val, val1, uintOp)
			} else {
				uint1Check, valid1 := val1.AsUint()
				if !valid1 || uint1Check > math.MaxInt64 {
					return NewInvalidFastVal()
				}
				return genericFastVal2IntsOp(val, val1, intOp)
			}
		case IntValue:
			fallthrough
		case JsonIntValue:
			return genericFastVal2IntsOp(val, val1, intOp)
		case FloatValue:
			fallthrough
		case JsonFloatValue:
			return genericFastVal2FloatsOp(val, val1, floatOp)
		default:
			return NewInvalidFastVal()
		}
	case FloatValue:
		fallthrough
	case JsonFloatValue:
		return genericFastVal2FloatsOp(val, val1, floatOp)
	default:
		return NewInvalidFastVal()
	}
}

func FastValMathAdd(val, val1 FastVal) FastVal {
	return fastValMathAddMultSharedLogic(val, val1, fastValMathAddInt, fastValMathAddUint, fastValMathAdd)
}

func FastValMathMul(val, val1 FastVal) FastVal {
	return fastValMathAddMultSharedLogic(val, val1, fastValMathMultInt, fastValMathMultUint, fastValMathMult)
}

func FastValMathSub(val, val1 FastVal) FastVal {
	switch val.dataType {
	case UintValue:
		fallthrough
	case JsonUintValue:
		switch val1.dataType {
		case UintValue:
			fallthrough
		case JsonUintValue:
			uintCheck, valid := val.AsUint()
			uint1Check, valid1 := val1.AsUint()
			if !valid || !valid1 {
				return NewInvalidFastVal()
			}
			if uintCheck >= uint1Check {
				return genericFastVal2UintsOp(val, val1, fastValMathSubUint)
			} else {
				// Negative result
				return genericFastVal2IntsOp(val, val1, fastValMathSubInt)
			}
		case IntValue:
			fallthrough
		case JsonIntValue:
			uintCheck, valid := val.AsUint()
			int1Check, valid1 := val1.AsInt()
			if !valid || !valid1 {
				return NewInvalidFastVal()
			}
			if int1Check >= 0 {
				int1AsUint, valid := val1.AsUint()
				if !valid {
					return NewInvalidFastVal()
				}
				if int1AsUint > uintCheck {
					// Result will be negative
					return genericFastVal2IntsOp(val, val1, fastValMathSubInt)
				} else {
					return genericFastVal2UintsOp(val, val1, fastValMathSubUint)
				}
			} else {
				// Subtracting a neg int == adding int to uint to be prevent potential overflow
				positiveIntFastVal := NewIntFastVal(fastValNegateInt(int1Check))
				return genericFastVal2UintsOp(val, positiveIntFastVal, fastValMathAddUint)
			}
		case FloatValue:
			fallthrough
		case JsonFloatValue:
			return genericFastVal2FloatsOp(val, val1, fastValMathSub)
		default:
			return NewInvalidFastVal()
		}
	case IntValue:
		fallthrough
	case JsonIntValue:
		switch val1.dataType {
		case UintValue:
			fallthrough
		case JsonUintValue:
			intCheck, valid := val.AsInt()
			uint1Check, valid1 := val1.AsUint()
			if !valid || !valid1 {
				return NewInvalidFastVal()
			}
			if uint1Check > math.MaxInt64 {
				// Instead of invalid - best effort and let float take care of it
				return genericFastVal2FloatsOp(val, val1, fastValMathSub)
			}
			if intCheck >= 0 {
				uint1AsInt, _ := val1.AsInt()
				if intCheck > uint1AsInt {
					// positive result
					return genericFastVal2UintsOp(val, val1, fastValMathSubUint)
				} else {
					return genericFastVal2IntsOp(val, val1, fastValMathSubInt)
				}
			} else {
				// Result will be negative
				return genericFastVal2IntsOp(val, val1, fastValMathSubInt)
			}
		case IntValue:
			fallthrough
		case JsonIntValue:
			return genericFastVal2IntsOp(val, val1, fastValMathSubInt)
		case FloatValue:
			fallthrough
		case JsonFloatValue:
			return genericFastVal2FloatsOp(val, val1, fastValMathSub)
		default:
			return NewInvalidFastVal()
		}
	case FloatValue:
		fallthrough
	case JsonFloatValue:
		return genericFastVal2FloatsOp(val, val1, fastValMathSub)
	default:
		return NewInvalidFastVal()
	}
}

func FastValMathDiv(val, val1 FastVal) FastVal {
	switch val.dataType {
	case UintValue:
		fallthrough
	case JsonUintValue:
		switch val1.dataType {
		case UintValue:
			fallthrough
		case JsonUintValue:
			val1Check, valid := val1.AsUint()
			if !valid || val1Check == 0 {
				return NewInvalidFastVal()
			}
			return genericFastVal2UintsOp(val, val1, fastValMathDivUint)
		case IntValue:
			fallthrough
		case JsonIntValue:
			int1Check, valid1 := val1.AsInt()
			if !valid1 || int1Check == 0 {
				return NewInvalidFastVal()
			} else if int1Check > 0 {
				return genericFastVal2UintsOp(val, val1, fastValMathDivUint)
			} else {
				return genericFastVal2IntsOp(val, val1, fastValMathDivInt)
			}
		case FloatValue:
			fallthrough
		case JsonFloatValue:
			return genericFastVal2FloatsOp(val, val1, fastValMathDiv)
		default:
			return NewInvalidFastVal()
		}
	case IntValue:
		fallthrough
	case JsonIntValue:
		intCheck, valid := val.AsInt()
		if !valid {
			return NewInvalidFastVal()
		}
		switch val1.dataType {
		case UintValue:
			fallthrough
		case JsonUintValue:
			uint1Check, valid1 := val1.AsUint()
			if !valid1 || intCheck < 0 && uint1Check > math.MaxInt64 {
				// Cannot convert to int type
				return genericFastVal2FloatsOp(val, val1, fastValMathDiv)
			} else {
				if intCheck >= 0 {
					return genericFastVal2UintsOp(val, val1, fastValMathDivUint)
				} else {
					return genericFastVal2IntsOp(val, val1, fastValMathDivInt)
				}
			}
		case IntValue:
			fallthrough
		case JsonIntValue:
			return genericFastVal2IntsOp(val, val1, fastValMathDivInt)
		case FloatValue:
			fallthrough
		case JsonFloatValue:
			return genericFastVal2FloatsOp(val, val1, fastValMathDiv)
		default:
			return NewInvalidFastVal()
		}
	case FloatValue:
		fallthrough
	case JsonFloatValue:
		return genericFastVal2FloatsOp(val, val1, fastValMathDiv)
	default:
		return NewInvalidFastVal()
	}
}

func FastValMathMod(val, val1 FastVal) FastVal {
	switch val.dataType {
	case UintValue:
		fallthrough
	case JsonUintValue:
		switch val1.dataType {
		case UintValue:
			fallthrough
		case JsonUintValue:
			val1Check, valid := val1.AsUint()
			if !valid || val1Check == 0 {
				return NewInvalidFastVal()
			}
			return genericFastVal2UintsOp(val, val1, fastValMathModUint)
		case IntValue:
			fallthrough
		case JsonIntValue:
			int1Check, valid1 := val1.AsInt()
			if !valid1 || int1Check == 0 {
				return NewInvalidFastVal()
			} else if int1Check > 0 {
				return genericFastVal2UintsOp(val, val1, fastValMathModUint)
			} else {
				return genericFastVal2IntsOp(val, val1, fastValMathMod)
			}
		default:
			return NewInvalidFastVal()
		}
	case IntValue:
		fallthrough
	case JsonIntValue:
		intCheck, valid := val.AsInt()
		if !valid {
			return NewInvalidFastVal()
		}
		switch val1.dataType {
		case UintValue:
			fallthrough
		case JsonUintValue:
			uint1Check, valid1 := val1.AsUint()
			if !valid1 || intCheck < 0 && uint1Check > math.MaxInt64 {
				// Cannot convert to int type
				return NewInvalidFastVal()
			} else {
				if intCheck >= 0 {
					return genericFastVal2UintsOp(val, val1, fastValMathModUint)
				} else {
					return genericFastVal2IntsOp(val, val1, fastValMathMod)
				}
			}
		case IntValue:
			fallthrough
		case JsonIntValue:
			return genericFastVal2IntsOp(val, val1, fastValMathMod)
		default:
			return NewInvalidFastVal()
		}
	default:
		return NewInvalidFastVal()
	}
}

func FastValMathNeg(val FastVal) FastVal {
	switch val.dataType {
	case UintValue:
		fallthrough
	case JsonUintValue:
		checkUint, _ := val.AsUint()
		if checkUint > math.MaxInt64 {
			return NewInvalidFastVal()
		}
		fallthrough
	case IntValue:
		fallthrough
	case JsonIntValue:
		return genericFastValIntOp(val, fastValNegateInt)
	case FloatValue:
		fallthrough
	case JsonFloatValue:
		return genericFastValFloatOp(val, fastValNegate)
	default:
		return NewInvalidFastVal()
	}
}
