// Copyright 2018-2019 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFilterExpressionParser(t *testing.T) {
	assert := assert.New(t)
	parser, err := NewFilterExpressionParser("")
	assert.Nil(err)
	assert.NotNil(parser)

	fe := &FilterExpression{}
	err = parser.ParseString("TRUE OR FALSE AND NOT FALSE", fe)
	assert.Nil(err)
	assert.Equal(2, len(fe.AndConditions))
	assert.Equal(1, len(fe.AndConditions[0].OrConditions))
	assert.Equal(2, len(fe.AndConditions[1].OrConditions))
	assert.NotNil(fe.AndConditions[1].OrConditions[1].Not)

	fe = &FilterExpression{}
	err = parser.ParseString("((TRUE OR FALSE)) OR (TRUE)", fe)
	assert.Nil(err)
	assert.Equal(3, len(fe.AndConditions))
	assert.Equal(1, len(fe.AndConditions[0].OrConditions))
	assert.Equal(1, len(fe.AndConditions[1].OrConditions))
	assert.Equal(1, len(fe.AndConditions[2].OrConditions))
	assert.Equal(2, len(fe.AndConditions[0].OrConditions[0].PreParen))
	assert.Equal(2, len(fe.AndConditions[1].OrConditions[0].PostParen))
	assert.Equal(1, len(fe.AndConditions[2].OrConditions[0].PreParen))
	assert.Equal(1, len(fe.AndConditions[2].OrConditions[0].PostParen))

	fe = &FilterExpression{}
	err = parser.ParseString("(TRUE AND FALSE) OR (FALSE)", fe)
	assert.Nil(err)
	assert.Equal(2, len(fe.AndConditions))
	assert.Equal(2, len(fe.AndConditions[0].OrConditions))
	assert.Equal(1, len(fe.AndConditions[0].OrConditions[0].PreParen))
	assert.Equal(1, len(fe.AndConditions[0].OrConditions[1].PostParen))
	assert.Equal(1, len(fe.AndConditions[1].OrConditions[0].PreParen))
	assert.Equal(1, len(fe.AndConditions[1].OrConditions[0].PostParen))

	fe = &FilterExpression{}
	err = parser.ParseString("NOT ((TRUE AND FALSE) OR (NOT (FALSE OR TRUE)))", fe)
	assert.Nil(err)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path >= field2", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.AndConditions[0].OrConditions[0].Operand.Op.IsGreaterThanOrEqualTo())
	assert.False(fe.AndConditions[0].OrConditions[0].Operand.Op.IsGreaterThan())
	assert.Equal("field2", fe.AndConditions[0].OrConditions[0].Operand.RHS.Field.String())

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path IS NOT NULL", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.AndConditions[0].OrConditions[0].Operand.CheckOp.IsNotNull())

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path = \"value\"", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.AndConditions[0].OrConditions[0].Operand.Op.IsEqual())
	assert.Equal("value", fe.AndConditions[0].OrConditions[0].Operand.RHS.Value.String())

	fe = &FilterExpression{}
	err = parser.ParseString("`onePath.Only` < field2", fe)
	assert.Nil(err)
	assert.Equal("onePath.Only", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("field2", fe.AndConditions[0].OrConditions[0].Operand.RHS.Field.String())

	fe = &FilterExpression{}
	err = parser.ParseString("`onePath.Only` <> \"value\"", fe)
	assert.Nil(err)
	assert.Equal("onePath.Only", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.True(fe.AndConditions[0].OrConditions[0].Operand.Op.IsNotEqual())
	assert.Equal("value", fe.AndConditions[0].OrConditions[0].Operand.RHS.Value.String())

	fe = &FilterExpression{}
	err = parser.ParseString("META().`onePath.Only` == \"value\"", fe)
	assert.Nil(err)
	assert.Equal("META()", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("onePath.Only", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.AndConditions[0].OrConditions[0].Operand.Op.IsEqual())
	assert.Equal("value", fe.AndConditions[0].OrConditions[0].Operand.RHS.Value.String())

	fe = &FilterExpression{}
	err = parser.ParseString("`[$%XDCRInternalMeta*%$]`.metaKey = \"testKey\"", fe)
	assert.Nil(err)
	assert.Equal("[$%XDCRInternalMeta*%$]", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("metaKey", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.AndConditions[0].OrConditions[0].Operand.Op.IsEqual())
	assert.Equal("testKey", fe.AndConditions[0].OrConditions[0].Operand.RHS.Value.String())

	// path name with leading number must be escaped - TODO this should be documented
	fe = &FilterExpression{}
	err = parser.ParseString("`2DarrayPath`[1][-2] = fieldpath2.path2", fe)
	assert.Nil(err)
	assert.Equal("2DarrayPath [1] [-2]", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.True(fe.AndConditions[0].OrConditions[0].Operand.Op.IsEqual())

	fe = &FilterExpression{}
	err = parser.ParseString("arrayPath[1].path2.arrayPath3[-10].`multiword array`[20] = fieldpath2.path2", fe)
	assert.Nil(err)
	assert.Equal("arrayPath [1]", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("arrayPath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].StrValue)
	assert.Equal("path2", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].StrValue)
	assert.Equal(0, len(fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].ArrayIndexes))
	assert.Equal("arrayPath3 [-10]", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[2].String())
	assert.Equal("multiword array [20]", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[3].String())

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path = PI()", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(*fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncNoArg.ConstFuncNoArgName.Pi)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path <= ABS(5)", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.AndConditions[0].OrConditions[0].Operand.Op.IsLessThanOrEqualTo())
	assert.Equal("ABS", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Equal("5", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.String())
	assert.Nil(fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path = DATE(\"2019-01-01\")", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.Equal("DATE", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Equal("2019-01-01", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.String())
	assert.Nil(fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path = DATE(`field with spaces`)", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.Equal("DATE", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Equal("field with spaces", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.Field.String())
	assert.Nil(fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path >= ABS(CEIL(PI()))", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.Equal("ABS", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Nil(fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.Argument)
	assert.NotNil(fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc)
	assert.NotNil(fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc.ConstFuncOneArg)
	assert.NotNil(fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncOneArg.Argument.SubFunc.ConstFuncOneArg.Argument.SubFunc.ConstFuncNoArg)

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path != POW(ABS(CEIL(PI())),2)", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.True(fe.AndConditions[0].OrConditions[0].Operand.Op.IsNotEqual())
	assert.Equal("POW", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncTwoArgs.ConstFuncTwoArgsName.String())
	assert.Equal("ABS", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncTwoArgs.Argument0.SubFunc.ConstFuncOneArg.ConstFuncOneArgName.String())

	fe = &FilterExpression{}
	err = parser.ParseString("REGEX_CONTAINS(`[$%XDCRInternalKey*%$]`, \"^xyz*\")", fe)
	assert.Nil(err)
	assert.Equal("REGEX_CONTAINS", fe.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.BooleanFuncTwoArgsName.String())
	assert.Equal("[$%XDCRInternalKey*%$]", fe.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument0.Field.String())
	assert.Equal("^xyz*", fe.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument1.Argument.String())

	fe = &FilterExpression{}
	err = parser.ParseString("fieldpath.path = POW(ABS(CEIL(PI())),2) AND REGEX_CONTAINS(fieldPath2, \"^abc*$\")", fe)
	assert.Nil(err)
	assert.Equal("fieldpath", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[0].String())
	assert.Equal("path", fe.AndConditions[0].OrConditions[0].Operand.LHS.Field.Path[1].String())
	assert.Equal("POW", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncTwoArgs.ConstFuncTwoArgsName.String())
	assert.Equal("ABS", fe.AndConditions[0].OrConditions[0].Operand.RHS.Func.ConstFuncTwoArgs.Argument0.SubFunc.ConstFuncOneArg.ConstFuncOneArgName.String())
	assert.Equal(1, len(fe.AndConditions))
	assert.Equal(2, len(fe.AndConditions[0].OrConditions))
	assert.NotNil(fe.AndConditions[0].OrConditions[1].Operand.BooleanExpr)
	assert.Equal("fieldPath2", fe.AndConditions[0].OrConditions[1].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument0.String())
	assert.Equal("^abc*$", fe.AndConditions[0].OrConditions[1].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument1.Argument.String())
}
