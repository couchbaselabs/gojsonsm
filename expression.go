// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
	"strings"
)

func fieldExprCompare(lhs FieldExpr, rhs FieldExpr) bool {
	if rhs.Root != lhs.Root {
		return false
	}

	if len(rhs.Path) != len(lhs.Path) {
		return false
	}

	for i, path := range rhs.Path {
		if lhs.Path[i] != path {
			return false
		}
	}

	return true
}

func rootSetAddOne(set []FieldExpr, item FieldExpr) []FieldExpr {
	for _, oitem := range set {
		if fieldExprCompare(oitem, item) {
			return set
		}
	}
	return append(set, item)
}

func rootSetAdd(set []FieldExpr, items ...FieldExpr) []FieldExpr {
	out := set
	for _, item := range items {
		out = rootSetAddOne(out, item)
	}
	return out
}

type Expression interface {
	String() string
	RootRefs() []FieldExpr
}

type TrueExpr struct {
}

func (expr TrueExpr) String() string {
	return "True"
}

func (expr TrueExpr) RootRefs() []FieldExpr {
	return nil
}

type FalseExpr struct {
}

func (expr FalseExpr) String() string {
	return "True"
}

func (expr FalseExpr) RootRefs() []FieldExpr {
	return nil
}

type ValueExpr struct {
	Value interface{}
}

func (expr ValueExpr) String() string {
	return fmt.Sprintf("%v", expr.Value)
}

func (expr ValueExpr) RootRefs() []FieldExpr {
	return nil
}

type AndExpr []Expression

func (expr AndExpr) String() string {
	if len(expr) == 0 {
		return "%%ERROR%%"
	} else if len(expr) == 1 {
		return expr[0].String()
	} else {
		value := reindentString(expr[0].String(), "  ")
		for i := 1; i < len(expr); i++ {
			value += "\nAND\n"
			value += reindentString(expr[i].String(), "  ")
		}
		return value
	}
}

func (expr AndExpr) RootRefs() []FieldExpr {
	var out []FieldExpr
	for _, subexpr := range expr {
		out = rootSetAdd(out, subexpr.RootRefs()...)
	}
	return out
}

type OrExpr []Expression

func (expr OrExpr) String() string {
	if len(expr) == 0 {
		return "%%ERROR%%"
	} else if len(expr) == 1 {
		return expr[0].String()
	} else {
		value := reindentString(expr[0].String(), "  ")
		for i := 1; i < len(expr); i++ {
			value += "\nOR\n"
			value += reindentString(expr[i].String(), "  ")
		}
		return value
	}
}

func (expr OrExpr) RootRefs() []FieldExpr {
	var out []FieldExpr
	for _, subexpr := range expr {
		out = rootSetAdd(out, subexpr.RootRefs()...)
	}
	return out
}

type FieldExpr struct {
	Root int
	Path []string
}

func (expr FieldExpr) String() string {
	rootStr := "$doc"
	if expr.Root != 0 {
		rootStr = fmt.Sprintf("$%d", expr.Root)
	}

	if len(expr.Path) > 0 {
		return rootStr + "." + strings.Join(expr.Path, ".")
	} else {
		return rootStr
	}
}

func (expr FieldExpr) RootRefs() []FieldExpr {
	if expr.Root != 0 {
		return nil
	}
	return []FieldExpr{expr}
}

type AnyInExpr struct {
	VarId   int
	InExpr  Expression
	SubExpr Expression
}

func (expr AnyInExpr) String() string {
	exprStr := reindentString(expr.SubExpr.String(), "  ")
	return fmt.Sprintf("any $%d in %s\n%s\nend", expr.VarId, expr.InExpr, exprStr)
}

func (expr AnyInExpr) RootRefs() []FieldExpr {
	var out []FieldExpr
	out = rootSetAdd(out, expr.InExpr.RootRefs()...)
	out = rootSetAdd(out, expr.SubExpr.RootRefs()...)
	return out
}

type EveryInExpr struct {
	VarId   int
	InExpr  Expression
	SubExpr Expression
}

func (expr EveryInExpr) String() string {
	exprStr := reindentString(expr.SubExpr.String(), "  ")
	return fmt.Sprintf("every $%d in %s\n%s\nend", expr.VarId, expr.InExpr, exprStr)
}

func (expr EveryInExpr) RootRefs() []FieldExpr {
	var out []FieldExpr
	out = rootSetAdd(out, expr.InExpr.RootRefs()...)
	out = rootSetAdd(out, expr.SubExpr.RootRefs()...)
	return out
}

type EqualsExpr struct {
	Lhs Expression
	Rhs Expression
}

func (expr EqualsExpr) String() string {
	return fmt.Sprintf("%s = %s", expr.Lhs, expr.Rhs)
}

func (expr EqualsExpr) RootRefs() []FieldExpr {
	var out []FieldExpr
	out = rootSetAdd(out, expr.Lhs.RootRefs()...)
	out = rootSetAdd(out, expr.Rhs.RootRefs()...)
	return out
}

type LessThanExpr struct {
	Lhs Expression
	Rhs Expression
}

func (expr LessThanExpr) String() string {
	return fmt.Sprintf("%s < %s", expr.Lhs, expr.Rhs)
}

func (expr LessThanExpr) RootRefs() []FieldExpr {
	var out []FieldExpr
	out = rootSetAdd(out, expr.Lhs.RootRefs()...)
	out = rootSetAdd(out, expr.Rhs.RootRefs()...)
	return out
}

type GreaterEqualExpr struct {
	Lhs Expression
	Rhs Expression
}

func (expr GreaterEqualExpr) String() string {
	return fmt.Sprintf("%s >= %s", expr.Lhs, expr.Rhs)
}

func (expr GreaterEqualExpr) RootRefs() []FieldExpr {
	var out []FieldExpr
	out = rootSetAdd(out, expr.Lhs.RootRefs()...)
	out = rootSetAdd(out, expr.Rhs.RootRefs()...)
	return out
}

type InExpr struct {
	Lhs Expression
	Rhs []interface{}
}
