package gojsonsm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Makes sure that the parsing of subcontext works
func TestSimpleParserSubContext1(t *testing.T) {
	fmt.Println("Running TestSimpleParserSubContext1")
	assert := assert.New(t)

	testString := "true || firstName == 'Neil'"
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
	fmt.Println("Running TestSimpleParserSubContext2")
	assert := assert.New(t)

	testString := "true && firstName == 'Neil' || age < 50"
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
	fmt.Println("Running TestSimpleParserSubContext2a")
	assert := assert.New(t)

	testString := "(true && firstName == 'Neil') || age < 50"
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
	fmt.Println("Running TestSimpleParserSubContext3")
	assert := assert.New(t)

	testString := "firstName == 'Neil' && age < 50"
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

func TestSimpleParserSubContext3a(t *testing.T) {
	fmt.Println("Running TestSimpleParserSubContext3a")
	assert := assert.New(t)

	testString := "'Neil' == firstName && 50 > age"
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
	fmt.Println("Running TestSimpleParserSubContext4")
	assert := assert.New(t)

	testString := "firstName == 'Neil' && age < 50 || isActive == true"
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
	fmt.Println("Running TestSimpleParserSubContext4a")
	assert := assert.New(t)

	// This should have short circuiting -> firstName should be checked first
	testString := "firstName == 'Neil' && age < 50 && isActive == true"
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
	fmt.Println("Running TestSimpleParserSubContext4b")
	assert := assert.New(t)

	// Same as 4a but no short circuit eval
	testString := "firstName == 'Neil' && age < 50 && isActive == true"
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
	fmt.Println("Running TestSimpleParserSubContext5")
	assert := assert.New(t)

	testString := "((firstName == 'Neil'))"
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
	fmt.Println("Running TestSimpleParserSubContext6")
	assert := assert.New(t)

	testString := "firstName == 'Neil' && (age < 50 || isActive == true)"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
	node := ctx.parserDataNodes[ctx.subCtx.lastParserDataNode]
	assert.NotNil(node)

	//	fmt.Printf("DEBUG tree: %v\n", ctx.parserTree.data)
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
	fmt.Println("Running TestSimpleParserSubContext7")
	assert := assert.New(t)

	testString := "(firstName == 'Neil') && (age < 50 || isActive == true)"
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
	fmt.Println("Running TestSimpleParserSubContext7a")
	assert := assert.New(t)

	testString := "(firstName == 'Neil' )&& (age < 50 || isActive == true)"
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
	fmt.Println("Running TestContextShortCircuit1")
	assert := assert.New(t)
	testString := "firstName == 'Neil' || (age < 50) || (true)"
	ctx, _ := NewExpressionParserCtx(testString)

	ctx.enableShortCircuitEvalIfPossible()
	assert.True(ctx.shortCircuitEnabled)
}

func TestContextShortCircuit2(t *testing.T) {
	fmt.Println("Running TestContextShortCircuit2")
	assert := assert.New(t)
	testString := "firstName == 'Neil' || (age < 50) && (true)"
	ctx, _ := NewExpressionParserCtx(testString)

	ctx.enableShortCircuitEvalIfPossible()
	assert.False(ctx.shortCircuitEnabled)
}

func TestContextParserToken(t *testing.T) {
	fmt.Println("Running TestContextParserToken")
	assert := assert.New(t)
	testString := "firstName == 'Neil' || (age < 50) || (true)"
	ctx, err := NewExpressionParserCtx(testString)

	// firstName
	_, tokenType, err := ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	ctx.advanceToken()

	// ==
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Nil(err)
	ctx.advanceToken()

	// 'Neil'
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeValue))
	assert.Nil(err)
	ctx.advanceToken()

	// ||
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeOperator))
	assert.Nil(err)
	ctx.advanceToken()

	// (age -- will trim and will auto advance
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeParen))
	assert.Nil(err)

	// age
	_, tokenType, err = ctx.getCurrentToken()
	assert.Equal(tokenType, (ParseTokenType)(TokenTypeField))
	assert.Nil(err)
	//	fmt.Printf("age token: %v\n", token)
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
}

func TestSimpleParserCompare(t *testing.T) {
	fmt.Println("Running TestSimpleParserCompare")
	assert := assert.New(t)

	testString := "something >= somethingElse"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Nil(err)
}

func TestParserExpressionOutput(t *testing.T) {
	fmt.Println("Running TestParserExpressionOutput")
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

	strExpr := "isActive == true || age < 50"
	ctx, err := NewExpressionParserCtx(strExpr)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	assert.Equal(jsonExpr.String(), simpleExpr.String())
}

func TestParserExpressionOutput2(t *testing.T) {
	fmt.Println("Running TestParserExpressionOutput2")
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

	strExpr := "firstName == 'Neil' || (age < 50 && isActive == true)"
	varMap := make(FieldVarMap, 1)
	varMap["firstName"] = []string{"name", "first"}

	ctx, err := NewExpressionParserCtxWithFields(strExpr, varMap)
	assert.Nil(err)

	err = ctx.parse()
	assert.Nil(err)

	simpleExpr, err := ctx.outputExpression()
	assert.Nil(err)

	assert.Equal(jsonExpr.String(), simpleExpr.String())
}

// NEGATIVE test cases

// Test for when the first token is NOT a field value
func TestSimpleParserNeg(t *testing.T) {
	fmt.Println("Running TestSimpleParserNeg")
	assert := assert.New(t)

	testString := "|| true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg2(t *testing.T) {
	fmt.Println("Running TestSimpleParserNeg2")
	assert := assert.New(t)

	testString := "age < Neil == true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg3(t *testing.T) {
	fmt.Println("Running TestSimpleParserNeg3")
	assert := assert.New(t)

	testString := "something >= true"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg4(t *testing.T) {
	fmt.Println("Running TestSimpleParserNeg4")
	assert := assert.New(t)

	testString := ">= 2"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserNeg5(t *testing.T) {
	fmt.Println("Running TestSimpleParserNeg5")
	assert := assert.New(t)

	testString := "( true)&&( false)"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.NotNil(err)
}

func TestSimpleParserParenMismatch(t *testing.T) {
	fmt.Println("Running TestSimpleParserParenMismatch")
	assert := assert.New(t)

	testString := "(firstName == 'Neil'))"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Equal(ErrorParenMismatch, err)
}

func TestSimpleParserParenMismatch2(t *testing.T) {
	fmt.Println("Running TestSimpleParserParenMismatch2")
	assert := assert.New(t)

	testString := "((firstName == 'Neil')"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Equal(ErrorParenMismatch, err)
}

func TestSimpleParserParenMismatch3(t *testing.T) {
	fmt.Println("Running TestSimpleParserParenMismatch3")
	assert := assert.New(t)

	testString := ")>= 3"
	ctx, err := NewExpressionParserCtx(testString)
	assert.Equal(fieldMode, ctx.subCtx.currentMode)
	err = ctx.parse()
	assert.Equal(ErrorParenMismatch, err)
}
