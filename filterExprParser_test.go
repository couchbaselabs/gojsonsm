// Copyright 2018-2019 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFilterExpressionParser(t *testing.T) {
	assert := assert.New(t)
	parser, fe, err := NewFilterExpressionParser("`field` = TRUE")
	assert.Nil(err)
	assert.NotNil(parser)
	expr, err := fe.OutputExpression()
	assert.Nil(err)
	var trans Transformer
	matchDef := trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)

	err = parser.ParseString("TRUE OR FALSE AND NOT FALSE", fe)
	assert.Nil(err)
	assert.Equal(2, len(fe.FilterExpr.AndConditions))
	assert.Equal(1, len(fe.FilterExpr.AndConditions[0].OrConditions))
	assert.Equal(2, len(fe.FilterExpr.AndConditions[1].OrConditions))
	assert.NotNil(fe.FilterExpr.AndConditions[1].OrConditions[1].Not)
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("((TRUE OR FALSE))", fe)
	assert.Nil(err)
	assert.NotNil(fe.OpenParen)
	assert.NotNil(fe.CloseParen)
	assert.NotNil(fe.SubFilterExpr.OpenParen)
	assert.NotNil(fe.SubFilterExpr.CloseParen)
	assert.Equal(2, len(fe.SubFilterExpr.SubFilterExpr.FilterExpr.AndConditions))
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("(TRUE AND FALSE)", fe)
	assert.Nil(err)
	assert.NotNil(fe.OpenParen)
	assert.NotNil(fe.CloseParen)
	assert.Equal(1, len(fe.SubFilterExpr.FilterExpr.AndConditions))
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("(TRUE OR FALSE) AND (FALSE OR TRUE)", fe)
	assert.Nil(err)
	assert.NotNil(fe.OpenParen)
	assert.NotNil(fe.CloseParen)
	assert.NotNil(fe.AndContinuation)
	assert.NotNil(fe.AndContinuation.OpenParen)
	assert.NotNil(fe.AndContinuation.CloseParen)
	assert.Equal(2, len(fe.SubFilterExpr.FilterExpr.AndConditions))
	assert.Equal(2, len(fe.AndContinuation.SubFilterExpr.FilterExpr.AndConditions))
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("(TRUE OR FALSE) AND (FALSE OR TRUE) AND TRUE", fe)
	assert.Nil(err)
	assert.Equal(2, len(fe.SubFilterExpr.FilterExpr.AndConditions))
	assert.NotNil(fe.AndContinuation)
	assert.Equal(2, len(fe.AndContinuation.SubFilterExpr.FilterExpr.AndConditions))
	assert.Equal(1, len(fe.AndContinuation.AndContinuation.FilterExpr.AndConditions))
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("(TRUE AND FALSE) OR (FALSE AND TRUE)", fe)
	assert.Nil(err)
	assert.Equal(1, len(fe.SubFilterExpr.FilterExpr.AndConditions))
	assert.Equal(2, len(fe.SubFilterExpr.FilterExpr.AndConditions[0].OrConditions))
	assert.Equal(2, len(fe.OrContinuation.SubFilterExpr.FilterExpr.AndConditions[0].OrConditions))
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("((TRUE OR FALSE)) OR (TRUE)", fe)
	assert.Nil(err)
	assert.Equal(2, len(fe.SubFilterExpr.SubFilterExpr.FilterExpr.AndConditions))
	assert.NotNil(fe.OrContinuation)
	assert.Equal(1, len(fe.OrContinuation.SubFilterExpr.FilterExpr.AndConditions))
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("(TRUE AND FALSE) OR (FALSE)", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("TRUE AND (TRUE OR FALSE) AND FALSE", fe)
	assert.Nil(err)
	assert.Equal(1, len(fe.FilterExpr.AndConditions))
	assert.NotNil(fe.FilterExprContinuation)
	assert.Equal(2, len(fe.FilterExprContinuation.SubFilterExpr.FilterExpr.AndConditions))
	assert.NotNil(fe.FilterExprContinuation.AndContinuation)
	assert.Equal(1, len(fe.FilterExprContinuation.AndContinuation.FilterExpr.AndConditions))
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("(TRUE OR FALSE) AND (FALSE OR TRUE)", fe)
	assert.Nil(err)
	assert.Equal(2, len(fe.SubFilterExpr.FilterExpr.AndConditions))
	assert.NotNil(fe.AndContinuation)
	assert.Equal(2, len(fe.AndContinuation.SubFilterExpr.FilterExpr.AndConditions))
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("((TRUE OR FALSE) AND (FALSE OR TRUE)) OR (TRUE AND TRUE)", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("NOT NOT NOT TRUE", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path >= field2", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsGreaterThanOrEqualTo())
	assert.False(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsGreaterThan())
	assert.Equal("field2", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Field.String())

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path IS NOT NULL", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.CheckOp.IsNotNull())
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m := NewFastMatcher(matchDef)
	userData := map[string]interface{}{
		"fieldpath": map[string]interface{}{
			"path": 0,
		},
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path = \"value\"", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsEqual())
	assert.Equal("value", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Value.String())
	// Test double equal is the same as single eq
	err = parser.ParseString("fieldpath.path == \"value\"", fe)
	assert.Nil(err)
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsEqual())
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"fieldpath": map[string]interface{}{
			"path": "value",
		},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("`onePath.Only` < field2", fe)
	assert.Nil(err)
	assert.Equal("onePath.Only", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("field2", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Field.String())
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"onePath.Only": -2,
		"field2":       2,
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("EXISTS(onePath) AND onePath IS NOT NULL AND onePath.field1 < onePath.field2", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"onePath": map[string]interface{}{
			"field1": -2,
			"field2": 2e30,
		},
		"onePathCopy": map[string]interface{}{
			"field1": -2,
			"field2": 2e30,
		},
		"oneVar":  true,
		"oneList": []uint16{1, 2, 3, 4},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("EXISTS(onePath) AND onePath.field1 <> onePath.field2", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("EXISTS(oneVar)", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("EXISTS(onePath) AND EXISTS(onePath.field1) AND EXISTS(oneList)", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("onePath = onePathCopy", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("`onePath.Only` <> \"value\" OR `onePath.Only` <> \"value2\"", fe)
	assert.Nil(err)
	assert.Equal("onePath.Only", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsNotEqual())
	assert.Equal("value", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Value.String())
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"onePath.Only": -2,
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("META().`onePath.Only` = \"value\"", fe)
	assert.Nil(err)
	assert.Equal("META()", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("onePath.Only", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsEqual())
	assert.Equal("value", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Value.String())
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"META()": map[string]interface{}{
			"onePath.Only": "value",
		},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("`[$%XDCRInternalMeta*%$]`.metaKey = \"value\"", fe)
	assert.Equal("metaKey", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsEqual())
	assert.Equal("value", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Value.String())
	err = parser.ParseString("EXISTS (`[$%XDCRInternalMeta*%$]`.metaKey) AND `[$%XDCRInternalMeta*%$]`.metaKey = \"value\"", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"[$%XDCRInternalMeta*%$]": map[string]interface{}{
			"metaKey": "value",
		},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	// path name with leading number must be escaped - TODO this should be documented
	// We're not supporting neg index as of now
	fe = &FilterExpression{}
	err = parser.ParseString("`2DarrayPath`[1][-2] = fieldpath2.path2", fe)
	assert.NotNil(err)
	//	assert.Equal("2DarrayPath [1] [-2]", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	//	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsEqual())

	fe = &FilterExpression{}
	err = parser.ParseString("`1DarrayPath`[1] = \"arrayVal1\"", fe)
	assert.Nil(err)
	assert.Equal("1DarrayPath [1]", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsEqual())
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{"1DarrayPath": [2]string{"arrayVal0", "arrayVal1"}}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	// No negative indexes for now
	fe = &FilterExpression{}
	err = parser.ParseString("arrayPath[1].path2.arrayPath3[-10].`multiword array`[20] = fieldpath2.path2", fe)
	assert.NotNil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("arrayPath[1].path2.arrayPath3[10].`multiword array`[20] = fieldpath2.path2", fe)
	assert.Nil(err)
	assert.Equal("arrayPath [1]", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("arrayPath", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].StrValue.String())
	assert.Equal("path2", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].StrValue.String())
	assert.Equal(0, len(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].ArrayIndexes))
	assert.Equal("arrayPath3 [10]", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[2].String())
	assert.Equal("multiword array [20]", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[3].String())

	fe = &FilterExpression{}
	err = parser.ParseString("key < PI() AND -key < 0 AND key > -PI() AND key < ABS(-PI()) AND key > -ABS(-PI())", fe)
	assert.Nil(err)
	assert.Equal("key", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.True(*fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncNoArg.ConstFuncNoArgName.Pi)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{"key": 3.14}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path <= ABS(5)", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsLessThanOrEqualTo())
	assert.Equal("ABS", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Equal("5", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.String())
	assert.Nil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"fieldpath": map[string]interface{}{
			"path": -2,
		},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("DATE(fieldpath.path) = DATE(\"2019-01-01\")", fe)
	assert.Nil(err)
	assert.Equal("DATE", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Equal("2019-01-01", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.String())
	assert.Nil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"fieldpath": map[string]interface{}{
			"path": "2019-01-01",
		},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("DATE(fieldpath.path) > DATE(\"2019-01-01\") AND DATE(fieldpath.path) < DATE('2019-01-01T23:59:59.999Z') AND DATE(fieldpath.path) < DATE('2019-01-01T23:59:59.999-01:00')", fe)
	assert.Nil(err)
	assert.Equal("DATE", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Equal("2019-01-01", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.String())
	assert.Nil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"fieldpath": map[string]interface{}{
			"path": "2019-01-01 23:59:59",
		},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path = DATE(`field with spaces`)", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.Equal("DATE", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Equal("field with spaces", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.Field.String())
	assert.Nil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path >= ABS(CEIL(PI()))", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.Equal("ABS", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Nil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.Argument)
	assert.NotNil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc)
	assert.NotNil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc.ConstFuncOneArg)
	assert.NotNil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc.ConstFuncOneArg.Argument.SubFunc.ConstFuncNoArg)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"fieldpath": map[string]interface{}{
			"path": 10,
		},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)
	// Use same data as above, test IS NOT NULL
	err = parser.ParseString("fieldpath.path IS NOT NULL AND fieldpath.path IS NOT MISSING", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	match, err = m.Match(udMarsh)
	assert.True(match)

	// Test IS NULL
	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path IS NULL", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"fieldpath": map[string]interface{}{
			"path": nil,
		},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)
	// Use above data, test IS MISSING
	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path2 IS MISSING", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path <> POW(ABS(CEIL(PI())),2)", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsNotEqual())
	assert.Equal("POW", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncTwoArgs.ConstFuncTwoArgsName.String())
	assert.Equal("ABS", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncTwoArgs.Argument0.SubFunc.ConstFuncOneArg.ConstFuncOneArgName.String())
	// Test second not equals
	err = parser.ParseString("fieldpath.path != POW(ABS(CEIL(PI())),2)", fe)
	assert.Nil(err)
	assert.True(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.Op.IsNotEqual())

	fe = &FilterExpression{}
	err = parser.ParseString("REGEXP_CONTAINS(`[$%XDCRInternalKey*%$]`, \"^xyz*\")", fe)
	assert.Nil(err)
	assert.Equal("REGEXP_CONTAINS", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.BooleanFuncTwoArgsName.String())
	assert.Equal("[$%XDCRInternalKey*%$]", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument0.Field.String())
	assert.Equal("^xyz*", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument1.Argument.String())
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"[$%XDCRInternalKey*%$]": "xyzzzzz",
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("Testdoc = true AND REGEXP_CONTAINS(`[$%XDCRInternalKey*%$]`, \"^abc\")", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)

	testRaw := json.RawMessage(`{"Testdoc": true}`)
	testData, err := testRaw.MarshalJSON()
	tempMap := make(map[string]interface{})
	err = json.Unmarshal(testData, &tempMap)
	tempMap["[$%XDCRInternalKey*%$]"] = "abcdef"
	testData2, err := json.Marshal(tempMap)
	match, err = m.Match(testData2)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path = POW(ABS(CEIL(PI())),2) AND REGEXP_CONTAINS(fieldPath2, \"^abc*$\")", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.Equal("POW", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncTwoArgs.ConstFuncTwoArgsName.String())
	assert.Equal("ABS", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncTwoArgs.Argument0.SubFunc.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Equal(1, len(fe.FilterExpr.AndConditions))
	assert.Equal(2, len(fe.FilterExpr.AndConditions[0].OrConditions))
	assert.NotNil(fe.FilterExpr.AndConditions[0].OrConditions[1].Operand.BooleanExpr)
	assert.Equal("fieldPath2", fe.FilterExpr.AndConditions[0].OrConditions[1].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument0.String())
	assert.Equal("^abc*$", fe.FilterExpr.AndConditions[0].OrConditions[1].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument1.Argument.String())

	var testStr string = "`field.Path` = \"value\""
	_, err = GetFilterExpressionMatcher(testStr)
	assert.Nil(err)

	// MB-32987 - some combinations of nested arrays and objects tests
	fe = &FilterExpression{}
	err = parser.ParseString("achievements[0] = 49 AND achievements[1] = 58 AND achievements[2] = 108 AND arrOfObjs[0].`1D` = 50 AND floatArrs[0] = 1.1", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"category":     1,
		"email":        "3951c5@b1f2c7.com",
		"city":         "258171",
		"name":         "25134e ced17f",
		"coins":        354.32,
		"alt_email":    "5134ec@51c5b1.com",
		"body":         "testBody",
		"achievements": [6]int{49, 58, 108, 141, 177, 229},
		"floatArrs":    [6]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6},
		"arrOfObjs":    [1]map[string]interface{}{{"1D": 50}},
		"nestedArr":    [1][2]int{{61, 62}},
		"realm":        "f41e4a",
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)
	assert.Nil(err)

	// Check Exists on maps or arrays
	fe = &FilterExpression{}
	err = parser.ParseString("EXISTS(achievements) AND EXISTS(achievements[0])", fe)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	match, err = m.Match(udMarsh)
	assert.True(match)

	// MB-33014 - Numeric operation on a field
	fe = &FilterExpression{}
	err = parser.ParseString("achievements * 10 = 10", fe)
	assert.Nil(err)
	assert.Equal("achievements", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.NotNil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.MathOp.Multiply)
	assert.Equal("10", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Field.MathValue.String())
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"achievements": 1,
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("achievements * -10 = -10 AND achievements * -10.1 = -10.1", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"achievements": 1,
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("achievements < -1", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"achievements": -10,
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("achievements * -1 > -10", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"achievements": -10,
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("ABS(-achievements[2]*10) > 0", fe)
	assert.Nil(err)
	assert.NotNil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Func.ConstFuncOneArg.Argument.Field.MathNeg)
	assert.Equal("achievements", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Func.ConstFuncOneArg.Argument.Field.Path[0].StrValue.String())
	assert.Equal("[2]", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Func.ConstFuncOneArg.Argument.Field.Path[0].ArrayIndexes[0].String())
	assert.NotNil(fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Func.ConstFuncOneArg.Argument.Field.MathOp.Multiply)
	assert.Equal("10", fe.FilterExpr.AndConditions[0].OrConditions[0].Operand.LHS.Func.ConstFuncOneArg.Argument.Field.MathValue.String())
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	userData = map[string]interface{}{
		"achievements": [6]int{49, 58, 108, 141, 177, 229},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	// Beer sample
	beer := map[string]interface{}{
		"abv":         5.2,
		"brewery_id":  "big_buck_brewery",
		"category":    "North American Ale",
		"description": "A standard American-style beer and our flagship brand.  A small amount of corn is added to the grist to give the brew a smooth character.  Features a rich, golden color and a light malt character balanced with a mild dose of hops.",
		"ibu":         0,
		"name":        "Big Buck Beer",
		"srm":         0,
		"style":       "American-Style Pale Ale",
		"type":        "beer",
		"upc":         0,
		"updated":     "2019-03-22 20:00:20",
	}
	fe = &FilterExpression{}
	err = parser.ParseString("(country == \"United States\" OR country = \"Canada\" AND type=\"brewery\") OR (type=\"beer\" AND DATE(updated) >= DATE(\"2019-01-18\"))", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()

	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	udMarsh, _ = json.Marshal(beer)
	match, err = m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	err = parser.ParseString("((country == \"United States\" OR country = \"Canada\") AND type=\"brewery\") OR (type=\"beer\" AND DATE(updated) >= DATE(\"2019-01-18\"))", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()

	marshalledData := []byte(`{"[$%XDCRInternalKey*%$]":"big_buck_brewery-big_buck_beer","[$%XDCRInternalMeta*%$]":{},"abv":5.2,"brewery_id":"big_buck_brewery","category":"North American Ale","description":"A standard American-style beer and our flagship brand.  A small amount of corn is added to the grist to give the brew a smooth character.  Features a rich, golden color and a light malt character balanced with a mild dose of hops.","ibu":0,"name":"Big Buck Beer","srm":0,"style":"American-Style Pale Ale","type":"beer","upc":0,"updated":"2019-03-22 20:00:20"}`)
	fe = &FilterExpression{}
	err = parser.ParseString(`((county = "United States" OR country = "Canada") AND type="brewery") OR (type="beer" AND DATE(updated) >= DATE("2019-01-01"))`, fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	match, err = m.Match(marshalledData)
	assert.True(match)

	// Negative
	_, _, err = NewFilterExpressionParser("fieldpath.`path = fieldPath2")
	assert.NotNil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("(TRUE) OR FALSE)", fe)
	assert.NotNil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("(((TRUE) OR FALSE) OR FALSE))", fe)
	assert.NotNil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("TRUE", fe)
	assert.Nil(err)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	emptySlice := make([]byte, 0)
	match, err = m.Match(emptySlice)
	assert.False(match)
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("achievement * 2 +1", fe)
	assert.NotNil(err)

	// Typos
	_, _, err = NewFilterExpressionParser("REGEX_CONTAINS(KEY, \"something\")")
	assert.NotNil(err)
	_, _, err = NewFilterExpressionParser("REGEXP_CONTAINS(METAS().id, \"something\")")
	assert.NotNil(err)
	_, _, err = NewFilterExpressionParser("REGEXP_CONTAINS(METAS().ID(), \"something\")")
	assert.NotNil(err)

	// Unfinished
	_, _, err = NewFilterExpressionParser("REGEX_CONTAINS(KEY, \"something\") AND OR")
	assert.NotNil(err)
	_, _, err = NewFilterExpressionParser("REGEXP_CONTAINS(METAS().ID(), \"something)")
	assert.NotNil(err)
	_, _, err = NewFilterExpressionParser("`field is unfinished = \"unfinished_value")
	assert.NotNil(err)

	// Discontinued
	_, _, err = NewFilterExpressionParser("SomeKey EXISTS")
	assert.NotNil(err)

	// Invalid operators
	_, fe, err = NewFilterExpressionParser("field >< \"value\"")
	_, err = fe.OutputExpression()
	assert.NotNil(err)

	// Invalid date format
	_, fe, err = NewFilterExpressionParser(`DATE(updated) < DATE("2010-07-2220:22:20Z")`)
	_, err = fe.OutputExpression()
	assert.NotNil(err)

	// For invalid date values, it will be nil and should be smaller than any other valids
	fe = &FilterExpression{}
	err = parser.ParseString("DATE(fieldpath.path) < DATE(\"2019-01-01\")", fe)
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	matchDef = trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m = NewFastMatcher(matchDef)
	userData = map[string]interface{}{
		"fieldpath": map[string]interface{}{
			"path": "2019-01-01 27:59:59",
		},
	}
	udMarsh, _ = json.Marshal(userData)
	match, err = m.Match(udMarsh)
	assert.True(match)

	// Invalid parenthesis - this is caught by the parenthesis check
	_, fe, err = NewFilterExpressionParser("((TRUE OR FALSE () AND TRUE))")
	_, err = fe.OutputExpression()
	assert.NotNil(err)
}
