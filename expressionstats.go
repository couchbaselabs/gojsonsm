// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import "fmt"

type ExpressionStats struct {
	NumLoops       int
	NumNestedLoops int
	MaxLoopDepth   int
	NumAnds        int
	NumOrs         int
	NumFields      int
	NumValues      int
}

func (stats ExpressionStats) String() string {
	var out string
	out += fmt.Sprintf("num loops: %d\n", stats.NumLoops)
	out += fmt.Sprintf("num nested loops: %d\n", stats.NumNestedLoops)
	out += fmt.Sprintf("max loop depth: %d\n", stats.MaxLoopDepth)
	out += fmt.Sprintf("num ands: %d\n", stats.NumAnds)
	out += fmt.Sprintf("num ors: %d\n", stats.NumOrs)
	out += fmt.Sprintf("num fields: %d\n", stats.NumFields)
	out += fmt.Sprintf("num values: %d", stats.NumValues)
	return out
}

func (stats *ExpressionStats) scanOne(expr Expression, loopDepth int) error {
	if loopDepth > stats.MaxLoopDepth {
		stats.MaxLoopDepth = loopDepth
	}

	switch expr := expr.(type) {
	case FieldExpr:
		stats.NumFields++
	case ValueExpr:
		stats.NumValues++
	case AndExpr:
		stats.NumAnds++
		for _, subexpr := range expr {
			stats.scanOne(subexpr, loopDepth)
		}
	case OrExpr:
		stats.NumOrs++
		for _, subexpr := range expr {
			stats.scanOne(subexpr, loopDepth)
		}
	case AnyInExpr:
		stats.NumLoops++
		if loopDepth == 1 {
			stats.NumNestedLoops++
		}
		stats.scanOne(expr.InExpr, loopDepth)
		stats.scanOne(expr.SubExpr, loopDepth+1)
	case EveryInExpr:
		stats.NumLoops++
		if loopDepth == 1 {
			stats.NumNestedLoops++
		}
		stats.scanOne(expr.InExpr, loopDepth)
		stats.scanOne(expr.SubExpr, loopDepth+1)
	case EqualsExpr:
		stats.scanOne(expr.Lhs, loopDepth)
		stats.scanOne(expr.Rhs, loopDepth)
	case LessThanExpr:
		stats.scanOne(expr.Lhs, loopDepth)
		stats.scanOne(expr.Rhs, loopDepth)
	case GreaterEqualExpr:
		stats.scanOne(expr.Lhs, loopDepth)
		stats.scanOne(expr.Rhs, loopDepth)
	default:
		panic("unexpected expression type")
	}

	return nil
}

func (stats *ExpressionStats) Scan(expr Expression) error {
	return stats.scanOne(expr, 0)
}
