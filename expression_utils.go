package gojsonsm

import "fmt"

func fieldExprMatches(lhs FieldExpr, rhs FieldExpr) bool {
	if lhs.Root != rhs.Root {
		return false
	}
	if len(lhs.Path) != len(rhs.Path) {
		return false
	}
	for pathIdx := range lhs.Path {
		if lhs.Path[pathIdx] != rhs.Path[pathIdx] {
			return false
		}
	}
	return true
}

func fetchExprFieldRefsRecurse(expr Expression, loopVars []VariableID, fields []FieldExpr) []FieldExpr {
	switch expr := expr.(type) {
	case FieldExpr:
		isLoopVarRef := false
		for _, loopVar := range loopVars {
			if expr.Root == loopVar {
				isLoopVarRef = true
				break
			}
		}
		if isLoopVarRef {
			break
		}

		fieldAlreadyExists := false
		for _, oexpr := range fields {
			if fieldExprMatches(expr, oexpr) {
				fieldAlreadyExists = true
				break
			}
		}
		if fieldAlreadyExists {
			break
		}

		fields = append(fields, expr)
	case ValueExpr:
	case RegexExpr:
	case PcreExpr:
	case TimeExpr:
	case FuncExpr:
		for _, subexpr := range expr.Params {
			fields = fetchExprFieldRefsRecurse(subexpr, loopVars, fields)
		}
	case NotExpr:
		fields = fetchExprFieldRefsRecurse(expr.SubExpr, loopVars, fields)
	case AndExpr:
		for _, subexpr := range expr {
			fields = fetchExprFieldRefsRecurse(subexpr, loopVars, fields)
		}
	case OrExpr:
		for _, subexpr := range expr {
			fields = fetchExprFieldRefsRecurse(subexpr, loopVars, fields)
		}
	case AnyInExpr:
		fields = fetchExprFieldRefsRecurse(expr.InExpr, loopVars, fields)
		loopVars = append(loopVars, expr.VarId)
		fields = fetchExprFieldRefsRecurse(expr.SubExpr, loopVars, fields)
		loopVars = loopVars[0 : len(loopVars)-1]
	case EveryInExpr:
		fields = fetchExprFieldRefsRecurse(expr.InExpr, loopVars, fields)
		loopVars = append(loopVars, expr.VarId)
		fields = fetchExprFieldRefsRecurse(expr.SubExpr, loopVars, fields)
		loopVars = loopVars[0 : len(loopVars)-1]
	case AnyEveryInExpr:
		fields = fetchExprFieldRefsRecurse(expr.InExpr, loopVars, fields)
		loopVars = append(loopVars, expr.VarId)
		fields = fetchExprFieldRefsRecurse(expr.SubExpr, loopVars, fields)
		loopVars = loopVars[0 : len(loopVars)-1]
	case EqualsExpr:
		fields = fetchExprFieldRefsRecurse(expr.Lhs, loopVars, fields)
		fields = fetchExprFieldRefsRecurse(expr.Rhs, loopVars, fields)
	case NotEqualsExpr:
		fields = fetchExprFieldRefsRecurse(expr.Lhs, loopVars, fields)
		fields = fetchExprFieldRefsRecurse(expr.Rhs, loopVars, fields)
	case LessThanExpr:
		fields = fetchExprFieldRefsRecurse(expr.Lhs, loopVars, fields)
		fields = fetchExprFieldRefsRecurse(expr.Rhs, loopVars, fields)
	case LessEqualsExpr:
		fields = fetchExprFieldRefsRecurse(expr.Lhs, loopVars, fields)
		fields = fetchExprFieldRefsRecurse(expr.Rhs, loopVars, fields)
	case GreaterThanExpr:
		fields = fetchExprFieldRefsRecurse(expr.Lhs, loopVars, fields)
		fields = fetchExprFieldRefsRecurse(expr.Rhs, loopVars, fields)
	case GreaterEqualsExpr:
		fields = fetchExprFieldRefsRecurse(expr.Lhs, loopVars, fields)
		fields = fetchExprFieldRefsRecurse(expr.Rhs, loopVars, fields)
	case ExistsExpr:
		fields = fetchExprFieldRefsRecurse(expr.SubExpr, loopVars, fields)
	case LikeExpr:
		fields = fetchExprFieldRefsRecurse(expr.Lhs, loopVars, fields)
		fields = fetchExprFieldRefsRecurse(expr.Rhs, loopVars, fields)
	default:
		panic(fmt.Sprintf("unexpected expression type %T", expr))
	}

	return fields
}

func fetchExprFieldRefs(expr Expression) []FieldExpr {
	return fetchExprFieldRefsRecurse(expr, nil, nil)
}
