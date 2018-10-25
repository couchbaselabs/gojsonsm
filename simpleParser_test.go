// Copyright 2018 Couchbase, Inc. All rights reserved.

// +build !pcre

package gojsonsm

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContextParserToken(t *testing.T) {
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
	// Comment out as this will let us use "all" tag for testing
	// assert.Equal(ErrorPcreNotSupported, err)
	ctx.advanceToken()
}

func TestContextParserToken1(t *testing.T) {
	assert := assert.New(t)
	testString := "`name.[0]`"
	ctx, err := NewExpressionParserCtx(testString)

	// `name.[0]`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

}

func TestContextParserToken2(t *testing.T) {
	assert := assert.New(t)
	testString := "`name`[12]"
	ctx, err := NewExpressionParserCtx(testString)

	// `name`[0]
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	assert.Equal(2, len(ctx.lastFieldTokens))
	assert.Equal(ctx.lastFieldTokens[0], "name")
	assert.Equal(ctx.lastFieldTokens[1], "[12]")
}

func TestContextParserToken3(t *testing.T) {
	assert := assert.New(t)
	testString := "name[12][13]"
	ctx, err := NewExpressionParserCtx(testString)

	// `name`[0]
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	assert.Equal(3, len(ctx.lastFieldTokens))
	assert.Equal(ctx.lastFieldTokens[0], "name")
	assert.Equal(ctx.lastFieldTokens[1], "[12]")
	assert.Equal(ctx.lastFieldTokens[2], "[13]")
}

// Makes sure that the parsing of subcontext works
func TestSimpleParserSubContext1(t *testing.T) {
	assert := assert.New(t)

	testString := "true || `name`.`first` == \"Neil\""
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	assert.Equal(5, len(ctx.parserDataNodes))
	assert.Equal(5, ctx.parserTree.NumNodes())
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(1, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(3, ctx.parserTree.data[1].Right)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(1, ctx.parserTree.data[3].ParentIdx)
	assert.Equal(4, ctx.parserTree.data[3].Right)
	assert.Equal(2, ctx.parserTree.data[3].Left)
}

func TestSimpleParserSubContext2(t *testing.T) {
	assert := assert.New(t)

	testString := "true && `name`.`first` == \"Neil\" || `age` < 50"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(5, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(3, ctx.parserTree.data[1].Right)
	assert.Equal(1, ctx.parserTree.data[3].ParentIdx)
	assert.Equal(2, ctx.parserTree.data[3].Left)
	assert.Equal(4, ctx.parserTree.data[3].Right)
	assert.Equal(5, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[5].ParentIdx)
	assert.Equal(1, ctx.parserTree.data[5].Left)
	assert.Equal(7, ctx.parserTree.data[5].Right)
	assert.Equal(5, ctx.parserTree.data[7].ParentIdx)
	assert.Equal(6, ctx.parserTree.data[7].Left)
	assert.Equal(8, ctx.parserTree.data[7].Right)
}

func TestSimpleParserSubContext2a(t *testing.T) {
	assert := assert.New(t)

	testString := "(true && `name`.`first` == \"Neil\") || `age` < 50"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(5, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(3, ctx.parserTree.data[1].Right)
	assert.Equal(1, ctx.parserTree.data[3].ParentIdx)
	assert.Equal(2, ctx.parserTree.data[3].Left)
	assert.Equal(4, ctx.parserTree.data[3].Right)
	assert.Equal(5, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[5].ParentIdx)
	assert.Equal(1, ctx.parserTree.data[5].Left)
	assert.Equal(7, ctx.parserTree.data[5].Right)
	assert.Equal(5, ctx.parserTree.data[7].ParentIdx)
	assert.Equal(6, ctx.parserTree.data[7].Left)
	assert.Equal(8, ctx.parserTree.data[7].Right)
}
func TestSimpleParserSubContext3(t *testing.T) {
	assert := assert.New(t)

	testString := "`name`.`first` == \"Neil\" && `age` < 50"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(3, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(2, ctx.parserTree.data[1].Right)
	assert.Equal(3, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[3].ParentIdx)
	assert.Equal(1, ctx.parserTree.data[3].Left)
	assert.Equal(5, ctx.parserTree.data[3].Right)
	assert.Equal(3, ctx.parserTree.data[5].ParentIdx)
	assert.Equal(4, ctx.parserTree.data[5].Left)
	assert.Equal(6, ctx.parserTree.data[5].Right)
}

func TestSimpleParserSubContext4(t *testing.T) {
	assert := assert.New(t)

	testString := "`name`.`first` == \"Neil\" && `age` < 50 || `isActive` == true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(3, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(2, ctx.parserTree.data[1].Right)
	assert.Equal(7, ctx.parserTree.data[3].ParentIdx)
	assert.Equal(1, ctx.parserTree.data[3].Left)
	assert.Equal(5, ctx.parserTree.data[3].Right)
	assert.Equal(3, ctx.parserTree.data[5].ParentIdx)
	assert.Equal(4, ctx.parserTree.data[5].Left)
	assert.Equal(6, ctx.parserTree.data[5].Right)
	assert.Equal(7, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[7].ParentIdx)
	assert.Equal(3, ctx.parserTree.data[7].Left)
	assert.Equal(9, ctx.parserTree.data[7].Right)
	assert.Equal(7, ctx.parserTree.data[9].ParentIdx)
	assert.Equal(8, ctx.parserTree.data[9].Left)
	assert.Equal(10, ctx.parserTree.data[9].Right)
}

func TestSimpleParserSubContext4a(t *testing.T) {
	assert := assert.New(t)

	// This should have short circuiting -> `name`.`first` should be checked first
	testString := "`name`.`first` == \"Neil\" && `age` < 50 && `isActive` == true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	ctx.enableShortCircuitEvalIfPossible() // NOTE this call - usually wrapped in main func
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(3, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(2, ctx.parserTree.data[1].Right)
	assert.Equal(3, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[3].ParentIdx)
	assert.Equal(1, ctx.parserTree.data[3].Left)
	assert.Equal(7, ctx.parserTree.data[3].Right)
	assert.Equal(7, ctx.parserTree.data[5].ParentIdx)
	assert.Equal(4, ctx.parserTree.data[5].Left)
	assert.Equal(6, ctx.parserTree.data[5].Right)
	assert.Equal(3, ctx.parserTree.data[7].ParentIdx)
	assert.Equal(5, ctx.parserTree.data[7].Left)
	assert.Equal(9, ctx.parserTree.data[7].Right)
	assert.Equal(7, ctx.parserTree.data[9].ParentIdx)
	assert.Equal(8, ctx.parserTree.data[9].Left)
	assert.Equal(10, ctx.parserTree.data[9].Right)
}

func TestSimpleParserSubContext4b(t *testing.T) {
	assert := assert.New(t)

	// Same as 4a but no short circuit eval
	testString := "`name`.`first` == \"Neil\" && `age` < 50 && `isActive` == true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(3, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(2, ctx.parserTree.data[1].Right)
	assert.Equal(7, ctx.parserTree.data[3].ParentIdx)
	assert.Equal(1, ctx.parserTree.data[3].Left)
	assert.Equal(5, ctx.parserTree.data[3].Right)
	assert.Equal(3, ctx.parserTree.data[5].ParentIdx)
	assert.Equal(4, ctx.parserTree.data[5].Left)
	assert.Equal(6, ctx.parserTree.data[5].Right)
	assert.Equal(7, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[7].ParentIdx)
	assert.Equal(3, ctx.parserTree.data[7].Left)
	assert.Equal(9, ctx.parserTree.data[7].Right)
	assert.Equal(7, ctx.parserTree.data[9].ParentIdx)
	assert.Equal(8, ctx.parserTree.data[9].Left)
	assert.Equal(10, ctx.parserTree.data[9].Right)
}

func TestSimpleParserSubContext5(t *testing.T) {
	assert := assert.New(t)

	testString := "((`name`.`first` == \"Neil\"))"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(1, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(2, ctx.parserTree.data[1].Right)
}

func TestSimpleParserSubContext5a(t *testing.T) {
	assert := assert.New(t)

	testString := "( `name`.`first` == \"Neil\")"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(1, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(2, ctx.parserTree.data[1].Right)
}

func TestSimpleParserSubContext6(t *testing.T) {
	assert := assert.New(t)

	testString := "`name`.`first` == \"Neil\" && (`age` < 50 || `isActive` == true)"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(3, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(2, ctx.parserTree.data[1].Right)
	assert.Equal(3, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[3].ParentIdx)
	assert.Equal(1, ctx.parserTree.data[3].Left)
	assert.Equal(7, ctx.parserTree.data[3].Right)
	assert.Equal(7, ctx.parserTree.data[5].ParentIdx)
	assert.Equal(4, ctx.parserTree.data[5].Left)
	assert.Equal(6, ctx.parserTree.data[5].Right)
	assert.Equal(3, ctx.parserTree.data[7].ParentIdx)
	assert.Equal(5, ctx.parserTree.data[7].Left)
	assert.Equal(9, ctx.parserTree.data[7].Right)
	assert.Equal(7, ctx.parserTree.data[9].ParentIdx)
	assert.Equal(8, ctx.parserTree.data[9].Left)
	assert.Equal(10, ctx.parserTree.data[9].Right)
}

func TestSimpleParserSubContext7(t *testing.T) {
	assert := assert.New(t)

	testString := "(`name`.`first` == \"Neil\") && (`age` < 50 || `isActive` == true)"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(3, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(2, ctx.parserTree.data[1].Right)
	assert.Equal(3, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[3].ParentIdx)
	assert.Equal(1, ctx.parserTree.data[3].Left)
	assert.Equal(7, ctx.parserTree.data[3].Right)
	assert.Equal(7, ctx.parserTree.data[5].ParentIdx)
	assert.Equal(4, ctx.parserTree.data[5].Left)
	assert.Equal(6, ctx.parserTree.data[5].Right)
	assert.Equal(3, ctx.parserTree.data[7].ParentIdx)
	assert.Equal(5, ctx.parserTree.data[7].Left)
	assert.Equal(9, ctx.parserTree.data[7].Right)
	assert.Equal(7, ctx.parserTree.data[9].ParentIdx)
	assert.Equal(8, ctx.parserTree.data[9].Left)
	assert.Equal(10, ctx.parserTree.data[9].Right)
}

func TestSimpleParserSubContext7a(t *testing.T) {
	assert := assert.New(t)

	testString := "(`name`.`first` == \"Neil\" )&& (`age` < 50 || `isActive` == true)"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	assert.Equal(3, ctx.parserTree.data[1].ParentIdx)
	assert.Equal(0, ctx.parserTree.data[1].Left)
	assert.Equal(2, ctx.parserTree.data[1].Right)
	assert.Equal(3, ctx.treeHeadIndex)
	assert.Equal(-1, ctx.parserTree.data[3].ParentIdx)
	assert.Equal(1, ctx.parserTree.data[3].Left)
	assert.Equal(7, ctx.parserTree.data[3].Right)
	assert.Equal(7, ctx.parserTree.data[5].ParentIdx)
	assert.Equal(4, ctx.parserTree.data[5].Left)
	assert.Equal(6, ctx.parserTree.data[5].Right)
	assert.Equal(3, ctx.parserTree.data[7].ParentIdx)
	assert.Equal(5, ctx.parserTree.data[7].Left)
	assert.Equal(9, ctx.parserTree.data[7].Right)
	assert.Equal(7, ctx.parserTree.data[9].ParentIdx)
	assert.Equal(8, ctx.parserTree.data[9].Left)
	assert.Equal(10, ctx.parserTree.data[9].Right)
}

func TestContextShortCircuit1(t *testing.T) {
	assert := assert.New(t)
	testString := "`name`.`first` == \"Neil\" || (`age` < 50) || (true)"
	ctx, _ := NewExpressionParserCtx(testString)

	ctx.enableShortCircuitEvalIfPossible()
	assert.True(ctx.shortCircuitEnabled)
}

func TestContextShortCircuit2(t *testing.T) {
	assert := assert.New(t)
	testString := "name.first == \"Neil\" || (`age` < 50) && (true)"
	ctx, _ := NewExpressionParserCtx(testString)

	ctx.enableShortCircuitEvalIfPossible()
	assert.False(ctx.shortCircuitEnabled)
}

func TestContextParserMultiwordToken(t *testing.T) {
	assert := assert.New(t)
	testString := "`name`.`first` NOT LIKE \"abc\""
	ctx, err := NewExpressionParserCtx(testString)

	// `name`.`first`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// NOT LIKE
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("NOT_LIKE", token)
	assert.Nil(err)
	ctx.advanceToken()

	// abc
	token, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeRegex))
	assert.Nil(err)
	assert.Equal("abc", token)
}

func TestContextParserMultiwordToken2a(t *testing.T) {
	assert := assert.New(t)
	testString := "`[XDCRInternal]`.`Version` > 1.0"
	ctx, err := NewExpressionParserCtx(testString)

	// `[XDCRInternal]`.`Version`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// >
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal(">", token)
	assert.Nil(err)
	ctx.advanceToken()

	// 1.0
	token, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeValue))
	assert.Nil(err)
	assert.Equal("1.0", token)
}

func TestContextParserMultiwordToken2b(t *testing.T) {
	assert := assert.New(t)
	testString := "name.first IS NOT NULL"
	ctx, err := NewExpressionParserCtx(testString)

	// `name`.`first`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// IS NOT NULL
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("IS_NOT_NULL", token)
	assert.Nil(err)
}

func TestContextParserMultiwordToken2c(t *testing.T) {
	assert := assert.New(t)
	testString := "`name`.`first` IS NOT NULL && isActive == true"
	ctx, err := NewExpressionParserCtx(testString)

	// `name`.`first`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// IS NOT NULL
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("IS_NOT_NULL", token)
	assert.Nil(err)
	ctx.advanceToken()

	// &&
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Nil(err)
	ctx.advanceToken()

	// isActive
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// ==
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Nil(err)
	ctx.advanceToken()

	// true
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeTrue))
	assert.Nil(err)
}

func TestContextParserMultiwordToken3(t *testing.T) {
	assert := assert.New(t)
	testString := "`name`.`first` IS MISSING"
	ctx, err := NewExpressionParserCtx(testString)

	// `name`.`first`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// IS MISSING
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("IS_MISSING", token)
	assert.Nil(err)
}

func TestContextParserMatch(t *testing.T) {
	assert := assert.New(t)
	testString := "name.first LIKE \"Ne[a|i]l\""
	ctx, err := NewExpressionParserCtx(testString)

	// `name`.`first`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// LIKE
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("=~", token)
	assert.Nil(err)
	ctx.advanceToken()
	assert.True(ctx.subCtx.opTokenContext.isLikeOp())

	// Ne[a|i]l
	token, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeRegex))
	assert.Nil(err)
	assert.Equal("Ne[a|i]l", token)

}

func TestContextParserWSValues(t *testing.T) {
	assert := assert.New(t)
	testString := "name.first ==  \"Amgen Inc\""
	ctx, err := NewExpressionParserCtx(testString)

	// `name`.`first`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// ==
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("==", token)
	assert.Nil(err)
	ctx.advanceToken()

	// Amgen Inc
	token, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeValue))
	assert.Nil(err)
	assert.Equal("Amgen Inc", token)

}

func TestContextParserWSValues2(t *testing.T) {
	assert := assert.New(t)
	testString := `(company.name ==  "Amgen Inc") && true`
	ctx, err := NewExpressionParserCtx(testString)

	// (
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeParen))
	assert.Nil(err)

	// `company`.`name`
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// ==
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("==", token)
	assert.Nil(err)
	ctx.advanceToken()

	// Amgen Inc
	token, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeValue))
	assert.Nil(err)
	assert.Equal("Amgen Inc", token)
	ctx.advanceToken()

	// )
	token, tokenType, err = ctx.getCurrentToken()
	assert.Equal((ParseTokenType)(TokenTypeEndParen), tokenType)
	assert.Nil(err)
	ctx.advanceToken()

	// &&
	token, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("&&", token)
	assert.Nil(err)
	ctx.advanceToken()

}

func TestContextParserWSValuesWEmbeddedQuotes(t *testing.T) {
	assert := assert.New(t)
	testString := "company.name ==  \"\"dummyCorp\"\""
	ctx, err := NewExpressionParserCtx(testString)

	// `name`.`first`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// ==
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("==", token)
	assert.Nil(err)
	ctx.advanceToken()

	// "dummyCorp
	token, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeValue))
	assert.Nil(err)
	assert.Equal(`"dummyCorp"`, token)

}

func TestContextParserWSValuesWEmbeddedQuotes2(t *testing.T) {
	assert := assert.New(t)
	testString := "company.name ==  \"\"dummy space Corp\"\""
	ctx, err := NewExpressionParserCtx(testString)

	// `name`.`first`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// ==
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("==", token)
	assert.Nil(err)
	ctx.advanceToken()

	// "dummy space corp
	token, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeValue))
	assert.Nil(err)
	assert.Equal(`"dummy space Corp"`, token)
}

func TestContextParserWSValuesWEmbeddedQuotes2a(t *testing.T) {
	assert := assert.New(t)
	testString := `company.name ==  "'dummy space Corp'"`
	ctx, err := NewExpressionParserCtx(testString)

	// `name`.`first`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// ==
	token, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Equal("==", token)
	assert.Nil(err)
	ctx.advanceToken()

	// dummy space corp
	token, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeValue))
	assert.Nil(err)
	assert.Equal(`'dummy space Corp'`, token)
}

func TestSimpleParserCompare(t *testing.T) {
	assert := assert.New(t)

	testString := "`something` >= \"somethingElse\""
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
}

func TestParserExpressionOutput(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["or",
		["equals",
			["field", "isActive"],
			["value", true]
		],
		["lessthan",
			["field", "age"],
			["value", 50]
		]
	]`)
	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	strExpr := "isActive == true || `age` < 50"
	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	assert.Equal(jsonExpr.String(), simpleExpr.String())
}

func TestParserExpressionOutput2a(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "Neil"]
	  ],
	  ["and",
	    ["lessthan",
	      ["field", "age"],
	      ["value", 50]
	    ],
	    ["equals",
	      ["field", "isActive"],
	      ["value", true]
	    ]
	  ]
    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	strExpr := "name.first == \"Neil\" || (age < 50 && isActive == true)"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	m := NewMatcher(matchDef)

	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Neil",
		},
		"isActive": true,
		"age":      32,
	}
	udMarsh, err := json.Marshal(userData)
	if err != nil {
	}
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}

func TestParserExpressionOutputNot(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "Neal"]
	  ],
	  ["and",
	    ["not",
	      ["lessthan",
	        ["field", "age"],
	        ["value", 50]
	      ]
	    ],
	    ["equals",
	      ["field", "isActive"],
	      ["value", true]
	    ]
	  ]
    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{jsonExpr})
	assert.NotNil(matchDef)

	m := NewMatcher(matchDef)

	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Neil",
		},
		"isActive": false,
		"age":      32,
	}
	udMarsh, err := json.Marshal(userData)
	if err != nil {
	}
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.False(match)

}

func TestParserExpressionOutputNot2(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "Neal"]
	  ],
	  ["and",
	    ["not",
	      ["lessthan",
	        ["field", "age"],
	        ["value", 50]
	      ]
	    ],
	    ["equals",
	      ["field", "isActive"],
	      ["value", true]
	    ]
	  ]
    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{jsonExpr})
	assert.NotNil(matchDef)

	m := NewMatcher(matchDef)

	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Neil",
		},
		"isActive": true,
		"age":      50,
	}
	udMarsh, err := json.Marshal(userData)
	if err != nil {
	}
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}

func TestParserExpressionOutputNot3(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "David"]
	  ],
	  ["and",
	    ["lessthan",
	      ["field", "age"],
	      ["value", 50]
	    ],
		["notequals",
		  ["field", "isActive"],
		  ["value", true]
		]
	  ]
    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{jsonExpr})
	assert.NotNil(matchDef)

	m := NewMatcher(matchDef)

	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Goliath",
		},
		"isActive": false,
		"age":      49,
	}
	udMarsh, err := json.Marshal(userData)
	if err != nil {
	}
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)

	strExpr := "`name`.`first` == \"David\" || (`age` < 50 && `isActive` != true)"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	assert.Equal(jsonExpr.String(), simpleExpr.String())
}

func TestParserExpressionOutputGreaterThan(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "David"]
	  ],
	  ["and",
	    ["greaterthan",
	      ["field", "age"],
	      ["value", 50]
	    ],
	    ["equals",
	      ["field", "isActive"],
	      ["value", true]
	    ]
	  ]
    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{jsonExpr})
	assert.NotNil(matchDef)

	m := NewMatcher(matchDef)

	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Goliath",
		},
		"isActive": true,
		"age":      51,
	}
	udMarsh, err := json.Marshal(userData)
	if err != nil {
	}
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)

	strExpr := "name.first == \"David\" || (age > 50 && `isActive` == true)"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	assert.Equal(jsonExpr.String(), simpleExpr.String())
}

func TestParserExpressionOutputGreaterThanEquals(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "David"]
	  ],
	  ["and",
	    ["greaterequals",
	      ["field", "age"],
	      ["value", 50]
	    ],
	    ["equals",
	      ["field", "isActive"],
	      ["value", true]
	    ]
	  ]
    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{jsonExpr})
	assert.NotNil(matchDef)

	m := NewMatcher(matchDef)

	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Goliath",
		},
		"isActive": true,
		"age":      50,
	}
	udMarsh, err := json.Marshal(userData)
	if err != nil {
	}
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)

	strExpr := "`name`.`first` == \"David\" || (`age` >= 50 && `isActive` == true)"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	assert.Equal(jsonExpr.String(), simpleExpr.String())
}

func TestParserExpressionOutputLessThan(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "David"]
	  ],
	  ["and",
	    ["lessthan",
	      ["field", "age"],
	      ["value", 50]
	    ],
	    ["equals",
	      ["field", "isActive"],
	      ["value", true]
	    ]
	  ]
    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{jsonExpr})
	assert.NotNil(matchDef)

	m := NewMatcher(matchDef)

	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Goliath",
		},
		"isActive": true,
		"age":      49,
	}
	udMarsh, err := json.Marshal(userData)
	if err != nil {
	}
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)

	strExpr := "`name`.`first` == \"David\" || (`age` < 50 && `isActive` == true)"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	assert.Equal(jsonExpr.String(), simpleExpr.String())
}

func TestParserExpressionOutputLessThanEq(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "David"]
	  ],
	  ["and",
	    ["lessequals",
	      ["field", "age"],
	      ["value", 50]
	    ],
	    ["equals",
	      ["field", "isActive"],
	      ["value", true]
	    ]
	  ]
    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{jsonExpr})
	assert.NotNil(matchDef)

	m := NewMatcher(matchDef)

	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Goliath",
		},
		"isActive": true,
		"age":      50,
	}
	udMarsh, err := json.Marshal(userData)
	if err != nil {
	}
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)

	strExpr := "name.first == \"David\" || (age <= 50 && isActive == true)"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	assert.Equal(jsonExpr.String(), simpleExpr.String())
}

func TestParserExpressionOutputMatch(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
		["like",
		    ["field", "name", "first"],
		    ["regex", "Ne[a|i]l"]
	    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)
	strExpr := "`name`.`first` =~ \"Ne[a|i]l\""

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Neil",
		},
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}

func TestParserExpressionOutputMatchNeg(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
		["like",
		    ["field", "name", "first"],
		    ["regex", "Ne[a|i]l"]
	    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)
	strExpr := "`name`.`first` =~ \"Ne[a|i]l\""

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "David",
		},
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.False(match)
}

func TestParserExpressionOutputMatchNotNeg(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
		["not",
			["like",
			    ["field", "name", "first"],
			    ["regex", "Ne[a|i]l"]
			]
	    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)
	strExpr := "`name`.`first` NOT LIKE \"Ne[a|i]l\""

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "David",
		},
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}

func TestParserAlternativeOperators(t *testing.T) {
	assert := assert.New(t)
	strExpr := "name.first == \"David\" || (age < 50 && isActive != true)"
	strExpr2 := "`name`.`first` = \"David\" OR (`age` < 50 AND `isActive` != true)"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)
	ctx2, err := NewExpressionParserCtx(strExpr2)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)
	err = ctx2.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)
	simpleExpr2, err := ctx2.outputExpression()
	assert.Nil(err)

	assert.Equal(simpleExpr2.String(), simpleExpr.String())
}

func TestParserExpressionOutputNotMatch(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
		["not",
			["like",
			    ["field", "name", "first"],
			    ["regex", "Ne[a|i]l"]
			]
	    ]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)
	strExpr := "`name`.`first` NOT LIKE \"Ne[a|i]l\""

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Neil",
		},
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.False(match)
}

func TestParserExpressionOutputExists(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["exists",
		["field", "name", "first"]
	]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)
	strExpr := "name.first EXISTS"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": "Neil",
		},
	}

	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)

}

func TestParserExpressionOutputArrayEquals(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["equals",
		["field", "userIDs", "[1]"],
		["value", "nelio2k"]
	]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)
	strExpr := "userIDs[1] == \"nelio2k\""

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"userIDs": []string{
			"brett19",
			"nelio2k",
		},
	}

	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}

func TestParserExpressionOutputNotExists(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["notexists",
		["field", "name", "first"]
	]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)
	strExpr := "name.first IS MISSING"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"firstName": "Neil",
		},
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}

func TestParserExpressionOutputIsNull(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	  ["equals",
	    ["field", "name", "first"],
	    ["value", null]
	]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)
	strExpr := "name.first IS NULL"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": nil,
		},
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}

func TestParserExpressionOutputIsNotNull(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	  ["notequals",
	    ["field", "name", "first"],
	    ["value", null]
	]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)
	strExpr := "name.first IS NOT NULL"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"name": map[string]interface{}{
			"first": nil,
		},
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.False(match)
}

func TestParserExpressionOutputIsTrue(t *testing.T) {
	assert := assert.New(t)

	strExpr := "name == true"

	matchJson := []byte(`
	  ["equals",
	    ["field", "name"],
	    ["value", true]
	]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{jsonExpr})
	matchDef2 := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)
	assert.NotNil(matchDef2)

	m := NewMatcher(matchDef)
	m2 := NewMatcher(matchDef2)
	userData := map[string]interface{}{
		"name": true,
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
	match2, err := m2.Match(udMarsh)
	assert.Nil(err)
	assert.True(match2)
}

func TestParserExpressionOutputXDCRInternalObj(t *testing.T) {
	assert := assert.New(t)

	strExpr := "`[XDCRInternal]`.Version > 2.0"

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
		"[XDCRInternal]": map[string]interface{}{
			"Version": 3.0,
		},
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}

// NEGATIVE test cases
func TestSimpleParserParenMismatch(t *testing.T) {
	assert := assert.New(t)

	testString := "(`name`.`first` == \"Neil\"))"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Equal(ErrorParenMismatch, err)
}

func TestSimpleParserParenMismatch2(t *testing.T) {
	assert := assert.New(t)

	testString := "((`name`.`first` == \"Neil\")"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Equal(ErrorParenMismatch, err)
}

func TestSimpleParserParenMismatch3(t *testing.T) {
	assert := assert.New(t)

	testString := ")>= 3"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Equal(ErrorParenMismatch, err)
}

func TestSimpleParserParenSyntaxErr(t *testing.T) {
	assert := assert.New(t)

	testString := "(aField)> 3"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserParenSyntaxErr2(t *testing.T) {
	assert := assert.New(t)

	testString := "(someField == true)&& true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNoBacktickBegin(t *testing.T) {
	assert := assert.New(t)

	testString := "noBacktick` == true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNoBacktickEnd(t *testing.T) {
	assert := assert.New(t)

	testString := "`noBacktick == true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

// Test for when the first token is NOT a field value
func TestSimpleParserNeg(t *testing.T) {
	assert := assert.New(t)

	testString := "|| true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg2(t *testing.T) {
	assert := assert.New(t)

	testString := "`age` < Neil == true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg3(t *testing.T) {
	assert := assert.New(t)

	testString := "something >= true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg4(t *testing.T) {
	assert := assert.New(t)

	testString := ">= 2"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg5(t *testing.T) {
	assert := assert.New(t)

	testString := "( true)&&( false)"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg6(t *testing.T) {
	assert := assert.New(t)

	testString := "\"Neil\" == `name`.`first` && 50 > `age`"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg7(t *testing.T) {
	assert := assert.New(t)

	testString := "abc(def"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Equal(ErrorParenWSpace, err)
}

func TestSimpleParserNeg8(t *testing.T) {
	assert := assert.New(t)

	testString := "someField == true &&(def) == false"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg9(t *testing.T) {
	assert := assert.New(t)

	testString := ".somefield == 1"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg10(t *testing.T) {
	assert := assert.New(t)

	testString := "somefield. == 1"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg11(t *testing.T) {
	assert := assert.New(t)

	testString := "`field`[0a] == 1"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
	assert.Equal(err, ErrorLeadingZeroes)
}

func TestSimpleParserNeg12(t *testing.T) {
	assert := assert.New(t)

	testString := "`field`[01] == 1"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
	assert.Equal(err, ErrorLeadingZeroes)
}

func TestSimpleParserNeg13(t *testing.T) {
	assert := assert.New(t)

	testString := "`field`[] == 1"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
	assert.Equal(err, ErrorEmptyNest)
}

func TestSimpleParserNeg14(t *testing.T) {
	assert := assert.New(t)

	testString := "`field"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
	assert.Equal(err, ErrorMissingBacktickBracket)
}

func TestSimpleParserNeg15(t *testing.T) {
	assert := assert.New(t)

	testString := "`field`[12 == true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
	assert.Equal(err, ErrorMissingBacktickBracket)
}

func TestSimpleParserNeg17(t *testing.T) {
	assert := assert.New(t)

	testString := "`` == true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
	assert.Equal(ErrorEmptyLiteral, err)
}

func TestSimpleParserNeg18(t *testing.T) {
	assert := assert.New(t)

	testString := "field[ == true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
	assert.Equal(err, ErrorMissingBacktickBracket)
}

func TestSimpleParserNeg19(t *testing.T) {
	assert := assert.New(t)
	testString := "`name`[`first`]"

	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
	assert.Equal(err, ErrorAllInts)
}

func TestSimpleParserNeg20(t *testing.T) {
	assert := assert.New(t)
	testString := "field[invalid] == true"

	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
	assert.Equal(err, ErrorAllInts)
}

func TestParserExpressionOutputNeg(t *testing.T) {
	assert := assert.New(t)

	emptyString := ""
	ctx, err := NewExpressionParserCtx(emptyString)
	assert.Nil(err)

	_, err = ctx.outputExpression()
	assert.NotNil(err)
}

func TestParserExpressionWithGreaterThan(t *testing.T) {
	assert := assert.New(t)

	strExpr := "`age` > 50"

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	_, err = ctx.outputExpression()
	assert.Nil(err)
}

func TestContextParserNegMultiwordToken(t *testing.T) {
	assert := assert.New(t)
	testString := "name.first IS NOT LIKE \"abc\""
	ctx, err := NewExpressionParserCtx(testString)

	// `name`.`first`
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// NOT LIKE
	_, tokenType, err = ctx.getCurrentToken()
	assert.NotNil(err)
	ctx.advanceToken()
}

func TestParserExpressionOutputArrayEqualsMissing(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["equals",
		["field", "userIDs", "[1]"],
		["value", "nelio2k"]
	]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)
	strExpr := "userIDs[1] == \"nelio2k\""

	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{simpleExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"userIDsAlternate": []string{
			"brett19",
			"nelio2k",
		},
	}
	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.False(match)
}

func TestParserExpressionMathRoundValues(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
		["equals",
			["func", "mathRound",
				["field", "number"]
			],
			["value", 5]
		]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	strExpr := "ROUND(number) ==  5"
	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{jsonExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"number": 5.4,
	}

	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}

func TestParserExpressionMathRoundValues2(t *testing.T) {
	assert := assert.New(t)

	matchJson := []byte(`
	["equals",
		["func", "mathRound",
			["field", "number"]
		],
		["func", "mathRound",
			["value", 5]
		]
	]`)

	jsonExpr, err := ParseJsonExpression(matchJson)
	assert.Nil(err)

	strExpr := "ROUND(number) ==  ROUND(5)"
	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	var trans Transformer
	matchDef := trans.Transform([]Expression{jsonExpr})
	assert.NotNil(matchDef)

	assert.Equal(jsonExpr.String(), simpleExpr.String())

	m := NewMatcher(matchDef)
	userData := map[string]interface{}{
		"number": 5.4,
	}

	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}

func TestParserBunchaMathFuncs(t *testing.T) {
	assert := assert.New(t)

	strExpr := "ABS(negNum) ==  5 && SQRT(squaredNum) > 1"
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
		"negNum":     -5,
		"squaredNum": 4,
	}

	udMarsh, _ := json.Marshal(userData)
	match, err := m.Match(udMarsh)
	assert.Nil(err)
	assert.True(match)
}
