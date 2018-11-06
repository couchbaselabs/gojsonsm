// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
	"strings"
)

type VariableID int

func (id VariableID) String() string {
	if id == 0 {
		return "$doc"
	}

	return fmt.Sprintf("$%d", id)
}

type Expression interface {
	String() string
}

type TrueExpr struct {
}

func (expr TrueExpr) String() string {
	return "True"
}

type FalseExpr struct {
}

func (expr FalseExpr) String() string {
	return "False"
}

type ValueExpr struct {
	Value interface{}
}

func (expr ValueExpr) String() string {
	return fmt.Sprintf("%v", expr.Value)
}

type RegexExpr struct {
	Regex interface{}
}

func (expr RegexExpr) String() string {
	return fmt.Sprintf("/%v/", expr.Regex)
}

type PcreExpr struct {
	Pcre interface{}
}

func (expr PcreExpr) String() string {
	return fmt.Sprintf("/%v/", expr.Pcre)
}

type NotExpr struct {
	SubExpr Expression
}

func (expr NotExpr) String() string {
	return "NOT " + expr.SubExpr.String()
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

type FieldExpr struct {
	Root VariableID
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

type FuncExpr struct {
	FuncName string
	Params   []Expression
}

func (expr FuncExpr) String() string {
	rootStr := fmt.Sprintf("func:%s(", expr.FuncName)
	for i, param := range expr.Params {
		if i > 0 {
			rootStr += ","
		}
		rootStr += param.String()
	}
	rootStr += ")"
	return rootStr
}

type AnyInExpr struct {
	VarId   VariableID
	InExpr  Expression
	SubExpr Expression
}

func (expr AnyInExpr) String() string {
	exprStr := reindentString(expr.SubExpr.String(), "  ")
	return fmt.Sprintf("any $%d in %s\n%s\nend", expr.VarId, expr.InExpr, exprStr)
}

type EveryInExpr struct {
	VarId   VariableID
	InExpr  Expression
	SubExpr Expression
}

func (expr EveryInExpr) String() string {
	exprStr := reindentString(expr.SubExpr.String(), "  ")
	return fmt.Sprintf("every $%d in %s\n%s\nend", expr.VarId, expr.InExpr, exprStr)
}

type AnyEveryInExpr struct {
	VarId   VariableID
	InExpr  Expression
	SubExpr Expression
}

func (expr AnyEveryInExpr) String() string {
	exprStr := reindentString(expr.SubExpr.String(), "  ")
	return fmt.Sprintf("any and every $%d in %s\n%s\nend", expr.VarId, expr.InExpr, exprStr)
}

type ExistsExpr struct {
	SubExpr Expression
}

func (expr ExistsExpr) String() string {
	return fmt.Sprintf("%s EXISTS", expr.SubExpr)
}

type NotExistsExpr struct {
	SubExpr Expression
}

func (expr NotExistsExpr) String() string {
	return fmt.Sprintf("%s IS MISSING", expr.SubExpr)
}

type EqualsExpr struct {
	Lhs Expression
	Rhs Expression
}

func (expr EqualsExpr) String() string {
	return fmt.Sprintf("%s = %s", expr.Lhs, expr.Rhs)
}

type NotEqualsExpr struct {
	Lhs Expression
	Rhs Expression
}

func (expr NotEqualsExpr) String() string {
	return fmt.Sprintf("%s != %s", expr.Lhs, expr.Rhs)
}

type LessThanExpr struct {
	Lhs Expression
	Rhs Expression
}

func (expr LessThanExpr) String() string {
	return fmt.Sprintf("%s < %s", expr.Lhs, expr.Rhs)
}

type LessEqualsExpr struct {
	Lhs Expression
	Rhs Expression
}

func (expr LessEqualsExpr) String() string {
	return fmt.Sprintf("%s <= %s", expr.Lhs, expr.Rhs)
}

type GreaterThanExpr struct {
	Lhs Expression
	Rhs Expression
}

func (expr GreaterThanExpr) String() string {
	return fmt.Sprintf("%s > %s", expr.Lhs, expr.Rhs)
}

type GreaterEqualsExpr struct {
	Lhs Expression
	Rhs Expression
}

func (expr GreaterEqualsExpr) String() string {
	return fmt.Sprintf("%s >= %s", expr.Lhs, expr.Rhs)
}

type LikeExpr struct {
	Lhs Expression
	Rhs Expression
}

func (expr LikeExpr) String() string {
	return fmt.Sprintf("%s =~ %s", expr.Lhs, expr.Rhs)
}
