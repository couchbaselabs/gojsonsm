// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

func compactExpressionOr(expr OrExpr) Expression {
	var newOrExpr OrExpr
	for _, subExpr := range expr {
		newSubExpr := CompactExpression(subExpr)
		switch newSubExpr.(type) {
		case TrueExpr:
			return TrueExpr{}
		case FalseExpr:
			// Do Nothing
		default:
			newOrExpr = append(newOrExpr, newSubExpr)
		}
	}
	if len(newOrExpr) == 0 {
		return FalseExpr{}
	}
	return newOrExpr
}

func compactExpressionAnd(expr AndExpr) Expression {
	var newAndExpr AndExpr
	for _, subExpr := range expr {
		newSubExpr := CompactExpression(subExpr)
		switch newSubExpr.(type) {
		case TrueExpr:
			// Do nothing
		case FalseExpr:
			return FalseExpr{}
		default:
			newAndExpr = append(newAndExpr, newSubExpr)
		}
	}
	if len(newAndExpr) == 0 {
		return TrueExpr{}
	}
	return newAndExpr
}

func compactExpressionAnyIn(expr AnyInExpr) Expression {
	switch expr.SubExpr.(type) {
	case TrueExpr:
		return TrueExpr{}
	case FalseExpr:
		return FalseExpr{}
	}
	return expr
}

func compactExpressionEveryIn(expr EveryInExpr) Expression {
	switch expr.SubExpr.(type) {
	case TrueExpr:
		return TrueExpr{}
	case FalseExpr:
		return FalseExpr{}
	}
	return expr
}

// This really only compacts the initial components of the expression
// to catch cases where a TrueExpr exists really low.
// KNOWN NOT TO ALWAYS WORK!
func CompactExpression(expr Expression) Expression {
	switch expr := expr.(type) {
	case OrExpr:
		return compactExpressionOr(expr)
	case AndExpr:
		return compactExpressionAnd(expr)
	case AnyInExpr:
		return compactExpressionAnyIn(expr)
	case EveryInExpr:
		return compactExpressionEveryIn(expr)
	default:
		return expr
	}
}