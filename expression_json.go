// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"encoding/json"
	"errors"
)

func parseJsonValue(data []interface{}) (Expression, error) {
	return ValueExpr{
		data[1],
	}, nil
}

func parseJsonField(data []interface{}) (Expression, error) {
	var out FieldExpr
	pos := 1
	if dataRoot, ok := data[pos].(float64); ok {
		out.Root = int(dataRoot)
		pos++
	}
	for ; pos < len(data); pos++ {
		dataElem, ok := data[pos].(string)
		if !ok {
			return nil, errors.New("invalid field expression format")
		}
		out.Path = append(out.Path, dataElem)
	}

	return out, nil
}

func parseJsonEquals(data []interface{}) (Expression, error) {
	lhsData, ok := data[1].([]interface{})
	if !ok {
		return nil, errors.New("invalid equals expression lhs format")
	}

	rhsData, ok := data[2].([]interface{})
	if !ok {
		return nil, errors.New("invalid equals expression rhs format")
	}

	lhs, err := parseJsonSubexpr(lhsData)
	if err != nil {
		return nil, err
	}

	rhs, err := parseJsonSubexpr(rhsData)
	if err != nil {
		return nil, err
	}

	return EqualsExpr{lhs, rhs}, nil
}

func parseJsonLessThan(data []interface{}) (Expression, error) {
	lhsData, ok := data[1].([]interface{})
	if !ok {
		return nil, errors.New("invalid lessthan expression lhs format")
	}

	rhsData, ok := data[2].([]interface{})
	if !ok {
		return nil, errors.New("invalid lessthan expression rhs format")
	}

	lhs, err := parseJsonSubexpr(lhsData)
	if err != nil {
		return nil, err
	}

	rhs, err := parseJsonSubexpr(rhsData)
	if err != nil {
		return nil, err
	}

	return LessThanExpr{lhs, rhs}, nil
}

func parseJsonGreaterThan(data []interface{}) (Expression, error) {
	lhsData, ok := data[1].([]interface{})
	if !ok {
		return nil, errors.New("invalid greaterthan expression lhs format")
	}

	rhsData, ok := data[2].([]interface{})
	if !ok {
		return nil, errors.New("invalid greaterthan expression rhs format")
	}

	lhs, err := parseJsonSubexpr(lhsData)
	if err != nil {
		return nil, err
	}

	rhs, err := parseJsonSubexpr(rhsData)
	if err != nil {
		return nil, err
	}

	return GreaterThanExpr{lhs, rhs}, nil
}

func parseJsonNot(data []interface{}) (Expression, error) {
	var out NotExpr

	subexprData, ok := data[1].([]interface{})
	if !ok {
		return nil, errors.New("invalid not expression format")
	}

	subexpr, err := parseJsonSubexpr(subexprData)
	if err != nil {
		return nil, err
	}

	out.SubExpr = subexpr

	return out, nil
}

func parseJsonOr(data []interface{}) (Expression, error) {
	var out OrExpr
	for i := 1; i < len(data); i++ {
		subexprData, ok := data[i].([]interface{})
		if !ok {
			return nil, errors.New("invalid or expression format")
		}

		subexpr, err := parseJsonSubexpr(subexprData)
		if err != nil {
			return nil, err
		}

		out = append(out, subexpr)
	}
	return out, nil
}

func parseJsonAnd(data []interface{}) (Expression, error) {
	var out AndExpr
	for i := 1; i < len(data); i++ {
		subexprData, ok := data[i].([]interface{})
		if !ok {
			return nil, errors.New("invalid and expression format")
		}

		subexpr, err := parseJsonSubexpr(subexprData)
		if err != nil {
			return nil, err
		}

		out = append(out, subexpr)
	}
	return out, nil
}

func parseJsonLoop(data []interface{}) (int, Expression, Expression, error) {
	varId, ok := data[1].(float64)
	if !ok {
		return 0, nil, nil, errors.New("invalid anyin expression variable format")
	}

	lhsData, ok := data[2].([]interface{})
	if !ok {
		return 0, nil, nil, errors.New("invalid anyin expression lhs format")
	}

	subexprData, ok := data[3].([]interface{})
	if !ok {
		return 0, nil, nil, errors.New("invalid anyin expression subexpr format")
	}

	lhsExpr, err := parseJsonSubexpr(lhsData)
	if err != nil {
		return 0, nil, nil, err
	}

	subexprExpr, err := parseJsonSubexpr(subexprData)
	if err != nil {
		return 0, nil, nil, err
	}

	return int(varId), lhsExpr, subexprExpr, nil
}

func parseJsonAnyIn(data []interface{}) (Expression, error) {
	varID, lhsExpr, subexprExpr, err := parseJsonLoop(data)
	if err != nil {
		return nil, err
	}

	return AnyInExpr{varID, lhsExpr, subexprExpr}, nil
}

func parseJsonEveryIn(data []interface{}) (Expression, error) {
	varID, lhsExpr, subexprExpr, err := parseJsonLoop(data)
	if err != nil {
		return nil, err
	}

	return EveryInExpr{varID, lhsExpr, subexprExpr}, nil
}

func parseJsonAnyEveryIn(data []interface{}) (Expression, error) {
	varID, lhsExpr, subexprExpr, err := parseJsonLoop(data)
	if err != nil {
		return nil, err
	}

	return AnyEveryInExpr{varID, lhsExpr, subexprExpr}, nil
}

func parseJsonSubexpr(data []interface{}) (Expression, error) {
	exprType, ok := data[0].(string)
	if !ok {

		return nil, errors.New("invalid expression type format")
	}

	switch exprType {
	case "value":
		return parseJsonValue(data)
	case "field":
		return parseJsonField(data)
	case "not":
		return parseJsonNot(data)
	case "or":
		return parseJsonOr(data)
	case "and":
		return parseJsonAnd(data)
	case "anyin":
		return parseJsonAnyIn(data)
	case "everyin":
		return parseJsonEveryIn(data)
	case "anyeveryin":
		return parseJsonAnyEveryIn(data)
	case "equals":
		return parseJsonEquals(data)
	case "lessthan":
		return parseJsonLessThan(data)
	case "greaterthan":
		return parseJsonGreaterThan(data)
	}

	return nil, errors.New("invalid expression type")
}

func ParseJsonExpression(data []byte) (Expression, error) {
	var parsedData []interface{}
	err := json.Unmarshal(data, &parsedData)
	if err != nil {
		return nil, err
	}
	return parseJsonSubexpr(parsedData)
}
