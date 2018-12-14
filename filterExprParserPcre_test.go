// Copyright 2018 Couchbase, Inc. All rights reserved.

// +build pcre

package gojsonsm

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFilterExpressionParserPcre(t *testing.T) {
	assert := assert.New(t)
	fe := &FilterExpression{}
	_, fe, err := NewFilterExpressionParser("REGEXP_CONTAINS(`[$%XDCRInternalKey*%$]`, \"a(?=foo)\")")
	assert.Nil(err)
	assert.Equal("REGEXP_CONTAINS", fe.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.BooleanFuncTwoArgsName.String())
	assert.Equal("[$%XDCRInternalKey*%$]", fe.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument0.Field.String())
	assert.Equal("a(?=foo)", fe.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument1.Argument.String())
	expr, err := fe.OutputExpression()
	assert.Nil(err)
	assert.NotNil(expr)

	// test has no pcre support
	fe = &FilterExpression{}
	_, fe, err = NewFilterExpressionParser("REGEXP_CONTAINS(`[$%XDCRInternalKey*%$]`, \"q(?!uit)\")")
	assert.Nil(err)
	assert.Equal("REGEXP_CONTAINS", fe.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.BooleanFuncTwoArgsName.String())
	assert.Equal("[$%XDCRInternalKey*%$]", fe.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument0.Field.String())
	assert.Equal("q(?!uit)", fe.AndConditions[0].OrConditions[0].Operand.BooleanExpr.BooleanFunc.BooleanFuncTwoArgs.Argument1.Argument.String())
	expr, err = fe.OutputExpression()
	assert.Nil(err)
	assert.NotNil(expr)
	var trans Transformer
	matchDef := trans.Transform([]Expression{expr})
	assert.NotNil(matchDef)
	m := NewFastMatcher(matchDef)
	userData := map[string]interface{}{
		"[$%XDCRInternalKey*%$]": "quiz",
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.True(match)

	fe = &FilterExpression{}
	_, fe, err = NewFilterExpressionParser("REGEXP_CONTAINS(`[$%XDCRInternalKey*%$]`, \"^d\")")
	assert.Nil(err)
	_, err = fe.OutputExpression()
	assert.Nil(err)

}
