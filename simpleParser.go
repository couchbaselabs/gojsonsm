package gojsonsm

import (
	//	"errors"
	"fmt"
	"regexp"
	"strings"
)

var ErrorNotFound error = fmt.Errorf("Error: Specified resource was not found")
var ErrorNoMoreTokens error = fmt.Errorf("Error: No more token found")
var ErrorNeedToStartOneNewCtx error = fmt.Errorf("Error: Need to spawn one subcontext")
var ErrorNeedToStartNewCtx error = fmt.Errorf("Error: Need to spawn subcontext")
var ErrorParenMismatch error = fmt.Errorf("Error: Parenthesis mismatch")
var NonErrorOneLayerDone error = fmt.Errorf("One layer has finished")

type ParserTreeNode struct {
	tokenType ParseTokenType
	data      interface{}
}

func needToSpawnNewContext(err error) bool {
	return err == ErrorNeedToStartOneNewCtx || err == ErrorNeedToStartNewCtx
}

func NewParserTreeNode(tokenType ParseTokenType, data interface{}) ParserTreeNode {
	newNode := &ParserTreeNode{
		tokenType: tokenType,
		data:      data,
	}
	return *newNode
}

// Parse mode is within the context that a valid expression should be generically of the type of:
// field > op -> value
type parseMode int

const (
	fieldMode parseMode = iota
	opMode    parseMode = iota
	valueMode parseMode = iota
)

type parserSubContext struct {
	// Actual parser context
	currentMode            parseMode
	lastSubFieldNode       int // The last finished left side of the op
	skipAdvanceCurrentMode bool

	// For tree organization
	lastParserDataNode  int // Last inserted parser data node location
	lastBinTreeDataNode int // Last inserted parserTree data node location
	lastFieldIndex      int
	lastOpIndex         int
	lastValueIndex      int

	// Means that we should return as soon as the one layer of field -> op -> value is done
	oneLayerMode bool
}

func (subctx *parserSubContext) isUnused() bool {
	return subctx.lastFieldIndex == -1 && subctx.lastOpIndex == -1 && subctx.lastValueIndex == -1 && subctx.lastSubFieldNode == -1
}

func NewParserSubContext() *parserSubContext {
	subCtx := &parserSubContext{
		lastFieldIndex:   -1,
		lastOpIndex:      -1,
		lastValueIndex:   -1,
		lastSubFieldNode: -1,
		currentMode:      fieldMode,
	}
	return subCtx
}

func NewParserSubContextOneLayer() *parserSubContext {
	subCtx := NewParserSubContext()
	subCtx.oneLayerMode = true
	return subCtx
}

type expressionParserContext struct {
	// For token reading
	tokens            []string
	currentTokenIndex int
	skipAdvanceMode   bool

	// The levels of parenthesis currently discovered in the expression
	parenDepth int

	// Current sub context
	subCtx *parserSubContext

	// Final parser tree - binTree is the one that is used to keep track of tree structure
	// Each element of []ParserTreeNode corresponds to the # of element in parserTree.data
	parserTree      binParserTree
	parserDataNodes []ParserTreeNode
	treeHeadIndex   int
}

func NewExpressionParserCtx(strExpression string) (*expressionParserContext, error) {
	subCtx := NewParserSubContext()
	ctx := &expressionParserContext{
		tokens:        strings.Fields(strExpression),
		subCtx:        subCtx,
		treeHeadIndex: -1,
	}
	return ctx, nil
}

type ParseTokenType int

const (
	TokenTypeField    ParseTokenType = iota
	TokenTypeOperator ParseTokenType = iota
	TokenTypeValue    ParseTokenType = iota
	TokenTypeParen    ParseTokenType = iota
	TokenTypeEndParen ParseTokenType = iota
	TokenTypeTrue     ParseTokenType = iota
	TokenTypeFalse    ParseTokenType = iota
	TokenTypeInvalid  ParseTokenType = iota
)

func (ptt ParseTokenType) String() string {
	switch ptt {
	case TokenTypeField:
		return "TokenTypeField"
	case TokenTypeOperator:
		return "TokenTypeOperator"
	case TokenTypeValue:
		return "TokenTypeValue"
	case TokenTypeParen:
		return "TokenTypeParen"
	case TokenTypeEndParen:
		return "TokenTypeEndParen"
	case TokenTypeTrue:
		return "TokenTypeTrue"
	case TokenTypeFalse:
		return "TokenTypeFalse"
	case TokenTypeInvalid:
		return "TokenTypeInvalid"
	}
	return "Unknown"
}

// Operator types
const (
	TokenOperatorEqual         = "=="
	TokenOperatorOr            = "||"
	TokenOperatorAnd           = "&&"
	TokenOperatorLessThan      = "<"
	TokenOperatorLessThanEq    = "<="
	TokenOperatorGreaterThan   = ">"
	TokenOperatorGreaterThanEq = ">="
)

func tokenIsOpType(token string) bool {
	return token == TokenOperatorEqual || token == TokenOperatorOr || token == TokenOperatorAnd ||
		token == TokenOperatorLessThan || token == TokenOperatorLessThanEq || token == TokenOperatorGreaterThan ||
		token == TokenOperatorGreaterThanEq
}

func (ctx *expressionParserContext) advanceToken() error {
	ctx.currentTokenIndex = ctx.currentTokenIndex + 1

	if ctx.skipAdvanceMode {
		ctx.skipAdvanceMode = false
		return nil
	}

	// context mode transition
	switch ctx.subCtx.currentMode {
	case fieldMode:
		// After the field mode, the next token *must* be an op
		ctx.subCtx.currentMode = opMode
	case opMode:
		// After the op mode, the next mode should be value mode
		ctx.subCtx.currentMode = valueMode
	case valueMode:
		if ctx.subCtx.oneLayerMode {
			// One layer mode means that we should return as soon as this value is done so we can merge
			ctx.currentTokenIndex = ctx.currentTokenIndex - 1
			return NonErrorOneLayerDone
		} else {
			// After the value is finished, this subcompletion becomes a "field", so the next mode should be
			// a op
			ctx.subCtx.currentMode = opMode
		}
	default:
		return fmt.Errorf("Not implemented yet for mode transition %v", ctx.subCtx.currentMode)
	}
	return nil
}

func (ctx *expressionParserContext) handleOpenParen() error {
	// Strip the "(" from this token and into its own element
	ctx.tokens = append(ctx.tokens, "")
	ctx.tokens[ctx.currentTokenIndex] = strings.TrimPrefix(ctx.tokens[ctx.currentTokenIndex], "(")
	copy(ctx.tokens[ctx.currentTokenIndex+1:], ctx.tokens[ctx.currentTokenIndex:])
	ctx.tokens[ctx.currentTokenIndex] = "("
	ctx.parenDepth++
	// for open paren, internally advance so the next getToken will not get a paren
	ctx.currentTokenIndex = ctx.currentTokenIndex + 1
	return nil
}

func (ctx *expressionParserContext) handleCloseParen() error {
	// Strip the ")" from the end of this token and insert into its own element
	for strings.HasSuffix(ctx.tokens[ctx.currentTokenIndex], ")") {
		ctx.tokens = append(ctx.tokens, "")
		copy(ctx.tokens[ctx.currentTokenIndex+1:], ctx.tokens[ctx.currentTokenIndex:])
		ctx.tokens[ctx.currentTokenIndex] = strings.TrimSuffix(ctx.tokens[ctx.currentTokenIndex], ")")
		ctx.tokens[ctx.currentTokenIndex+1] = ")"
	}
	return nil
}

func (ctx *expressionParserContext) handleCloseParenBookKeeping() error {
	if ctx.parenDepth == 0 {
		return ErrorParenMismatch
	}
	ctx.parenDepth--
	return nil
}

func (ctx *expressionParserContext) getCurrentToken() (string, ParseTokenType, error) {
	if ctx.currentTokenIndex >= len(ctx.tokens) {
		return "", TokenTypeInvalid, ErrorNoMoreTokens
	}
	token := ctx.tokens[ctx.currentTokenIndex]

	// Values by def should be enclosed within single quotes
	valueRegex := regexp.MustCompile(`^\'.*\'$`)
	// Or Values can be int or floats by themselves (w/o alpha char)
	valueNumRegex := regexp.MustCompile(`^(-?)(0|([1-9][0-9]*))(\\.[0-9]+)?$`)

	if tokenIsOpType(token) {
		return token, TokenTypeOperator, nil
	} else if valueRegex.MatchString(token) {
		return token, TokenTypeValue, nil
	} else if valueNumRegex.MatchString(token) {
		return token, TokenTypeValue, nil
	} else if token == "true" {
		return token, TokenTypeTrue, nil
	} else if token == "false" {
		return token, TokenTypeFalse, nil
	} else if token != "(" && strings.HasPrefix(token, "(") {
		ctx.handleOpenParen()
		token = ctx.tokens[ctx.currentTokenIndex]
		return token, TokenTypeParen, nil
	} else if token != ")" && strings.HasSuffix(token, ")") {
		ctx.handleCloseParen()
		return ctx.getCurrentToken()
	} else if token == ")" {
		return token, TokenTypeEndParen, ctx.handleCloseParenBookKeeping()
	} else {
		return token, TokenTypeField, nil
	}
}

func (ctx *expressionParserContext) setupForToken(tokenType ParseTokenType) error {
	if ctx.parserTree.NumNodes() == 0 {
		// First time setup already done in constructor
		return nil
	}
	switch tokenType {
	case TokenTypeParen:
		// TODO - Need special handling
	}
	return nil
}

func (ctx *expressionParserContext) checkTokenTypeWithinContext(tokenType ParseTokenType) error {
	switch ctx.subCtx.currentMode {
	case fieldMode:
		if tokenType == TokenTypeParen {
			return ErrorNeedToStartNewCtx
		} else if tokenType != TokenTypeField && tokenType != TokenTypeTrue && tokenType != TokenTypeFalse {
			return fmt.Errorf("Error: For field mode, token must be field type - received %v", tokenType.String())
		}
	case opMode:
		if tokenType == TokenTypeEndParen {
			ctx.skipAdvanceMode = true
			return NonErrorOneLayerDone
		} else if tokenType != TokenTypeOperator {
			return fmt.Errorf("Error: For operator mode, token must be operator type - received %v", tokenType.String())
		}
	case valueMode:
		if tokenType == TokenTypeParen {
			return ErrorNeedToStartNewCtx
		} else if tokenType == TokenTypeField {
			return ErrorNeedToStartOneNewCtx
		} else if tokenType != TokenTypeValue && tokenType != TokenTypeTrue && tokenType != TokenTypeFalse {
			return fmt.Errorf("Error: For value mode, token must be value type - received %v", tokenType.String())
		}
	default:
		return fmt.Errorf("Error: Not implemented for tokenType: %v", tokenType.String())
	}

	return nil
}

func (ctx *expressionParserContext) insertNode(newNode ParserTreeNode) error {
	ctx.parserDataNodes = append(ctx.parserDataNodes, newNode)
	ctx.subCtx.lastParserDataNode = len(ctx.parserDataNodes) - 1

	// binTree representation
	newBinTreeNode := &binParserTreeNode{
		tokenType: newNode.tokenType,
	}
	newBinTreeNode.ParentIdx = -1
	newBinTreeNode.Left = -1
	newBinTreeNode.Right = -1

	ctx.parserTree.data = append(ctx.parserTree.data, *newBinTreeNode)
	ctx.subCtx.lastBinTreeDataNode = ctx.parserTree.NumNodes() - 1

	switch ctx.subCtx.currentMode {
	case fieldMode:
		ctx.subCtx.lastFieldIndex = ctx.subCtx.lastBinTreeDataNode
		ctx.subCtx.lastSubFieldNode = ctx.subCtx.lastBinTreeDataNode
		// FieldMode - nothing to do just record this as the last field
		//		fmt.Printf("LastfieldIndex: %v\n", ctx.subCtx.lastFieldIndex)
	case opMode:
		ctx.subCtx.lastOpIndex = ctx.subCtx.lastBinTreeDataNode
		//		fmt.Printf("LastOpIndex: %v\n", ctx.lastOpIndex)
		thisOpNode := &ctx.parserTree.data[ctx.subCtx.lastOpIndex]
		thisOpNode.Left = ctx.subCtx.lastSubFieldNode
		// OpMode means this node becomes the field mode's parent
		lastFieldNode := &ctx.parserTree.data[ctx.subCtx.lastSubFieldNode]
		lastFieldNode.ParentIdx = ctx.subCtx.lastOpIndex
		// If head hasnt' been set, this becomes the tree head
		if ctx.treeHeadIndex == -1 {
			ctx.treeHeadIndex = ctx.subCtx.lastOpIndex
		}
		ctx.subCtx.lastSubFieldNode = ctx.subCtx.lastOpIndex
	case valueMode:
		ctx.subCtx.lastValueIndex = ctx.subCtx.lastBinTreeDataNode
		//		fmt.Printf("LastValueIndex: %v\n", ctx.lastValueIndex)
		// Value mode means that the op's right is the value
		thisValueNode := &ctx.parserTree.data[ctx.subCtx.lastValueIndex]
		thisValueNode.ParentIdx = ctx.subCtx.lastOpIndex

		lastOpNode := &ctx.parserTree.data[ctx.subCtx.lastOpIndex]
		lastOpNode.Right = ctx.subCtx.lastValueIndex

		// Now that we have completed a sub-expression, the "op" becomes the head of the last completed nodes
		ctx.subCtx.lastSubFieldNode = ctx.subCtx.lastOpIndex

	default:
		return fmt.Errorf("Unsure how to insert into tree: %v", newNode)
	}

	return nil
}

// Given two sub contexts, one in the ctx, and the older one, merge them
func (ctx *expressionParserContext) mergeAndRestoreSubContexts(currentSubCtx *parserSubContext) error {
	// If older subctx is not used, don't merge - this means this started with a open paren
	if currentSubCtx.isUnused() {
		return nil
	}

	// Note that the subContext within *ctx is considered newer spawned
	lastOpNodeOfCurrentSubCtx := &ctx.parserTree.data[currentSubCtx.lastOpIndex]
	if lastOpNodeOfCurrentSubCtx == nil {
		return ErrorNotFound
	}

	lastOpNodeOfNewContext := &ctx.parserTree.data[ctx.subCtx.lastOpIndex]
	if lastOpNodeOfNewContext == nil {
		return ErrorNotFound
	}

	if lastOpNodeOfCurrentSubCtx.Right != -1 {
		return fmt.Errorf("Merging sub-context should expect value to be unassigned, but currently assigned to: %v", lastOpNodeOfCurrentSubCtx.Right)
	}
	lastOpNodeOfCurrentSubCtx.Right = ctx.subCtx.lastOpIndex

	if lastOpNodeOfNewContext.ParentIdx != -1 {
		return fmt.Errorf("Merging sub-context should expect parent to be unassigned, but currently assigned to: %v", lastOpNodeOfNewContext.ParentIdx)
	}
	lastOpNodeOfNewContext.ParentIdx = currentSubCtx.lastOpIndex

	// Once merge is done - restore original context
	*(ctx.subCtx) = *(currentSubCtx)
	return nil
}

func (ctx *expressionParserContext) parse() error {
	var token string
	var tokenType ParseTokenType
	var err error
	for ; ; err = ctx.advanceToken() {
		if err != nil {
			break
		}

		token, tokenType, err = ctx.getCurrentToken()
		if err != nil {
			break
		}

		// Context mode should match the token
		var newNode ParserTreeNode
		err = ctx.checkTokenTypeWithinContext(tokenType)
		if err == nil {
			newNode = NewParserTreeNode(tokenType, token)
			// Push the token into the correct location into the tree
			err = ctx.insertNode(newNode)
			if err != nil {
				break
			}
		} else if needToSpawnNewContext(err) {
			// Save current subContext
			currentSubCtx := NewParserSubContext()
			*currentSubCtx = *ctx.subCtx

			if err == ErrorNeedToStartNewCtx {
				// Starting a new context means the new sub context continues until it ends or hits an end paren
				ctx.subCtx = NewParserSubContext()
			} else {
				// One Context means a specific set of (field -> op -> value) only, and then get back to merge
				ctx.subCtx = NewParserSubContextOneLayer()
			}

			// Recursively do the next call
			err = ctx.parse()
			if err != nil && err != NonErrorOneLayerDone {
				break
			}

			err = ctx.mergeAndRestoreSubContexts(currentSubCtx)
			if err != nil {
				break
			}
		} else {
			break
		}

	}
	if err == ErrorNoMoreTokens {
		if ctx.parenDepth > 0 {
			err = ErrorParenMismatch
		} else {
			err = nil
		}
	}
	return err
}

func getTokenType(token string) (ParseTokenType, error) {
	if len(token) == 0 {
		return TokenTypeInvalid, fmt.Errorf("getTokenType error: Token is of 0 length")
	} else if tokenIsOpType(token) {
		return TokenTypeOperator, nil
	}
	return TokenTypeInvalid, fmt.Errorf("getTokenType error: Unable to parse token: %v", token)
}

func ParseSimpleExpression(strExpression string) (Expression, error) {
	var dummyExpression Expression

	// For the sake of simplicity, prepend "true ||" in front

	ctx, err := NewExpressionParserCtx(strExpression)
	err = ctx.parse()

	return dummyExpression, err
}
