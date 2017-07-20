package gojsonsm

import (
	"encoding/json"
	"errors"
	"strings"
)

type SlowMatcher struct {
	expr Expression
	vars map[int]interface{}
}

func NewSlowMatcher(expr Expression) *SlowMatcher {
	return &SlowMatcher{
		expr: expr,
	}
}

func (m *SlowMatcher) resolveFieldParam(expr FieldExpr) (interface{}, error) {
	rootVal := m.vars[expr.Root]

	curVal := rootVal
	for _, field := range expr.Path {
		if mapVal, ok := curVal.(map[string]interface{}); ok {
			curVal = mapVal[field]
		} else {
			return NewNullFastVal(), errors.New("invalid path")
		}
	}

	return curVal, nil
}

func (m *SlowMatcher) resolveParam(expr Expression) (interface{}, error) {
	switch expr := expr.(type) {
	case FieldExpr:
		return m.resolveFieldParam(expr)
	case ValueExpr:
		return expr.Value, nil
	}

	panic("unexpected param expression")
}

func (m *SlowMatcher) matchOrExpr(expr OrExpr) (bool, error) {
	for _, subexpr := range expr {
		res, err := m.matchOne(subexpr)
		if err != nil {
			return false, err
		}

		if res {
			return true, nil
		}
	}

	return false, nil
}

func (m *SlowMatcher) matchAndExpr(expr AndExpr) (bool, error) {
	if len(expr) == 0 {
		return false, nil
	}

	for _, subexpr := range expr {
		res, err := m.matchOne(subexpr)
		if err != nil {
			return false, err
		}

		if !res {
			return false, nil
		}
	}

	return true, nil
}

func (m *SlowMatcher) compareExprs(lhs Expression, rhs Expression) (int, error) {
	lhsVal, err := m.resolveParam(lhs)
	if err != nil {
		return 0, err
	}

	rhsVal, err := m.resolveParam(rhs)
	if err != nil {
		return 0, err
	}

	switch lhsVal := lhsVal.(type) {
	case string:
		switch rhsVal := rhsVal.(type) {
		case string:
			return strings.Compare(lhsVal, rhsVal), nil
		}
		return 0, errors.New("invalid type comparisons")
	case float64:
		switch rhsVal := rhsVal.(type) {
		case float64:
			if lhsVal < rhsVal {
				return -1, nil
			} else if lhsVal > rhsVal {
				return 1, nil
			}
			return 0, nil
		}
		return 0, errors.New("invalid type comparisons")
	case bool:
		switch rhsVal := rhsVal.(type) {
		case bool:
			if lhsVal == true && rhsVal == false {
				return 1, nil
			} else if lhsVal == false && rhsVal == true {
				return -1, nil
			}
			return 0, nil
		}
	}

	panic("unexpected lhs type")
}

func (m *SlowMatcher) matchAnyInExpr(expr AnyInExpr) (bool, error) {
	vals, err := m.resolveParam(expr.InExpr)
	if err != nil {
		return false, err
	}

	switch vals := vals.(type) {
	case map[string]interface{}:
		for _, val := range vals {
			m.vars[expr.VarId] = val
			res, err := m.matchOne(expr.SubExpr)
			delete(m.vars, expr.VarId)

			if err != nil {
				return false, err
			}

			if res {
				return true, nil
			}
		}

		return false, nil
	case []interface{}:
		for _, val := range vals {
			m.vars[expr.VarId] = val
			res, err := m.matchOne(expr.SubExpr)
			delete(m.vars, expr.VarId)

			if err != nil {
				return false, err
			}

			if res {
				return true, nil
			}
		}

		return false, nil
	}

	panic("unexpected any in param type")
}

func (m *SlowMatcher) matchEqualsExpr(expr EqualsExpr) (bool, error) {
	val, err := m.compareExprs(expr.Lhs, expr.Rhs)
	if err != nil {
		return false, err
	}

	return val == 0, nil
}

func (m *SlowMatcher) matchLessThanExpr(expr LessThanExpr) (bool, error) {
	val, err := m.compareExprs(expr.Lhs, expr.Rhs)
	if err != nil {
		return false, err
	}

	return val < 0, nil
}

func (m *SlowMatcher) matchGreaterEqualExpr(expr GreaterEqualExpr) (bool, error) {
	val, err := m.compareExprs(expr.Lhs, expr.Rhs)
	if err != nil {
		return false, err
	}

	return val >= 0, nil
}

func (m *SlowMatcher) matchOne(expr Expression) (bool, error) {
	switch expr := expr.(type) {
	case OrExpr:
		return m.matchOrExpr(expr)
	case AndExpr:
		return m.matchAndExpr(expr)
	case AnyInExpr:
		return m.matchAnyInExpr(expr)
	case EqualsExpr:
		return m.matchEqualsExpr(expr)
	case LessThanExpr:
		return m.matchLessThanExpr(expr)
	case GreaterEqualExpr:
		return m.matchGreaterEqualExpr(expr)
	}

	panic("unexpected expression")
}

func (m *SlowMatcher) Match(data []byte) (bool, error) {
	var parsedData interface{}
	if err := json.Unmarshal(data, &parsedData); err != nil {
		return false, err
	}

	if m.vars == nil {
		m.vars = make(map[int]interface{})
	}
	m.vars[0] = parsedData

	res, err := m.matchOne(m.expr)
	if err != nil {
		return false, err
	}

	delete(m.vars, 0)
	return res, nil
}
