// Copyright 2018 Couchbase, Inc. All rights reserved.

// +build pcre

package gojsonsm

import (
	"encoding/json"
	"github.com/glenn-brown/golang-pkg-pcre/src/pkg/pcre"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestPcre(t *testing.T) {
	assert := assert.New(t)

	testString := "afoobar"
	testPattern := "a(?=foo)"

	pregex := pcre.MustCompile(testPattern, 0)
	assert.NotNil(pregex)

	pcreMatcher := &pcre.Matcher{}
	pcreMatcher.ResetString(pregex, testString, 0)
	assert.True(pcreMatcher.Matches())

	//	lookAheadPattern := "(?=foo)"
	lookAheadPattern := "\\(\\?\\=.+\\)"
	lap := regexp.MustCompile(lookAheadPattern)
	assert.NotNil(lap)
	assert.True(lap.MatchString(testPattern))
	lookBehindPattern := "\\(\\?\\<.+\\)"
	lbp := regexp.MustCompile(lookBehindPattern)
	assert.NotNil(lbp)
	assert.True(lbp.MatchString("a(?<foo)"))
	negLookAheadPattern := "\\(\\?\\!.+\\)"
	nlap := regexp.MustCompile(negLookAheadPattern)
	assert.NotNil(nlap)
	assert.True(nlap.MatchString("a(?!foo)"))
	negLookBehindPattern := "\\(\\?\\<\\!.+\\)"
	nlbp := regexp.MustCompile(negLookBehindPattern)
	assert.NotNil(nlbp)
	assert.True(nlbp.MatchString("a(?<!foo)"))
}

func TestContextParserPcreToken(t *testing.T) {
	assert := assert.New(t)
	testString := "name.first == \"Neil\" || (age < 50) || (true) && `someStr` LIKE \"a(?<!foo)\""
	ctx, err := NewExpressionParserCtx(testString)

	// name.first
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	assert.Equal(2, len(ctx.lastFieldTokens))
	assert.Equal(ctx.lastFieldTokens[0], "name")
	assert.Equal(ctx.lastFieldTokens[1], "first")
	ctx.advanceToken()

	// ==
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Nil(err)
	ctx.advanceToken()

	// "Neil"
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeValue))
	assert.Nil(err)
	ctx.advanceToken()

	// ||
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Nil(err)
	ctx.advanceToken()

	// (`age` -- will trim and will auto advance
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeParen))
	assert.Nil(err)

	// `age`
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	//	fmt.Printf("`age` token: %v\n", token)
	ctx.advanceToken()

	// <
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Nil(err)
	ctx.advanceToken()

	// 50)
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeValue))
	assert.Nil(err)
	ctx.advanceToken()

	// )
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeEndParen))
	assert.Nil(err)
	ctx.advanceToken()

	// ||
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Nil(err)
	ctx.advanceToken()

	// (true -- will trim and auto advance
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeParen))
	assert.Nil(err)

	// true
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeTrue))
	assert.Nil(err)
	ctx.advanceToken()

	// )
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeEndParen))
	assert.Nil(err)
	ctx.advanceToken()

	// ||
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Nil(err)
	ctx.advanceToken()

	// SomeStr
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// LIKE
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Nil(err)
	ctx.advanceToken()

	// a(?<!foo)\
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypePcre))
	assert.Nil(err)
	ctx.advanceToken()
}

func TestParserExpressionPcre(t *testing.T) {
	assert := assert.New(t)

	strExpr := "pcreKey LIKE \"q(?!uit)\""

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"pcreKey": "quino",
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)

	m2 := NewMatcher(matchDef)
	userDataFalse := map[string]interface{}{
		"pcreKey": "quit",
	}
	udMarsh, _ = json.Marshal(userDataFalse)
	match, err = m2.Match(udMarsh)
	assert.Nil(err)
	assert.False(match)
}
