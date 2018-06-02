package gojsonsm

import (
	"fmt"
	"regexp"
	"strings"
)

var emptyExpression Expression
var ErrorNotFound error = fmt.Errorf("Error: Specified resource was not found")
var ErrorNoMoreTokens error = fmt.Errorf("Error: No more token found")
var ErrorNeedToStartOneNewCtx error = fmt.Errorf("Error: Need to spawn one subcontext")
var ErrorNeedToStartNewCtx error = fmt.Errorf("Error: Need to spawn subcontext")
var ErrorParenMismatch error = fmt.Errorf("Error: Parenthesis mismatch")
var NonErrorOneLayerDone error = fmt.Errorf("One layer has finished")

type FieldVarMap map[string][]string

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
	chainMode parseMode = iota
)

func (pm parseMode) String() string {
	switch pm {
	case fieldMode:
		return "fieldMode"
	case opMode:
		return "opMode"
	case valueMode:
		return "valueMode"
	case chainMode:
		return "chainMode"
	default:
		return "Unknown"
	}
}

type opTokenContext int

const (
	noOp      opTokenContext = iota
	chainOp   opTokenContext = iota
	compareOp opTokenContext = iota
)

type parserSubContext struct {
	// Actual parser context
	currentMode            parseMode
	lastSubFieldNode       int // The last finished left side of the op
	skipAdvanceCurrentMode bool
	opTokenContext         opTokenContext

	// For tree organization
	lastParserDataNode  int // Last inserted parser data node location
	lastBinTreeDataNode int // Last inserted parserTree data node location
	lastFieldIndex      int
	lastOpIndex         int
	lastValueIndex      int

	// Means that we should return as soon as the one layer of field -> op -> value is done
	oneLayerMode bool

	// In case field and values entered are interchanged
	swapFieldAndVal bool
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

	// non-short ciruit eval -> left most expression does not necessarily translate into the first to be examined
	shortCircuitEnabled bool

	// Final parser tree - binTree is the one that is used to keep track of tree structure
	// Each element of []ParserTreeNode corresponds to the # of element in parserTree.data
	parserTree      binParserTree
	parserDataNodes []ParserTreeNode
	treeHeadIndex   int

	// Outputting context
	currentOuputNode int
	varMap           FieldVarMap
}

func NewExpressionParserCtxWithFields(strExpression string, varMap FieldVarMap) (*expressionParserContext, error) {
	ctx, err := NewExpressionParserCtx(strExpression)
	ctx.varMap = varMap
	return ctx, err
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
	TokenOperatorNotEqual      = "!="
	TokenOperatorOr            = "||"
	TokenOperatorAnd           = "&&"
	TokenOperatorLessThan      = "<"
	TokenOperatorLessThanEq    = "<="
	TokenOperatorGreaterThan   = ">"
	TokenOperatorGreaterThanEq = ">="
)

func tokenIsOpType(token string) bool {
	// Equal is both numeric and logical
	return tokenIsChainOpType(token) || token == TokenOperatorEqual || token == TokenOperatorNotEqual || tokenIsCompareOpType(token)
}

func tokenIsCompareOpType(token string) bool {
	return token == TokenOperatorGreaterThanEq || token == TokenOperatorLessThan || token == TokenOperatorLessThanEq || token == TokenOperatorGreaterThan
}

func tokenIsChainOpType(token string) bool {
	return token == TokenOperatorAnd || token == TokenOperatorOr
}

func (opCtx opTokenContext) isChainOp() bool {
	return opCtx == chainOp
}

func (opCtx opTokenContext) isCompareOp() bool {
	return opCtx == compareOp
}

func (opCtx *opTokenContext) clear() {
	if *opCtx != noOp {
		*opCtx = noOp
	}
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
	case chainMode:
		ctx.subCtx.currentMode = fieldMode
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
			ctx.subCtx.currentMode = chainMode
		}

		ctx.subCtx.opTokenContext.clear()
		ctx.subCtx.swapFieldAndVal = false
	default:
		return fmt.Errorf("Not implemented yet for mode transition %v", ctx.subCtx.currentMode)
	}
	return nil
}

func (ctx *expressionParserContext) handleParenPrefix(paren string) error {
	// Strip the "(" from this token and into its own element
	ctx.tokens = append(ctx.tokens, "")
	ctx.tokens[ctx.currentTokenIndex] = strings.TrimPrefix(ctx.tokens[ctx.currentTokenIndex], paren)
	copy(ctx.tokens[ctx.currentTokenIndex+1:], ctx.tokens[ctx.currentTokenIndex:])
	ctx.tokens[ctx.currentTokenIndex] = paren
	return nil
}

func (ctx *expressionParserContext) handleParenSuffix(paren string) error {
	// Strip the paren from the end of this token and insert into its own element
	for strings.HasSuffix(ctx.tokens[ctx.currentTokenIndex], paren) {
		ctx.tokens = append(ctx.tokens, "")
		copy(ctx.tokens[ctx.currentTokenIndex+1:], ctx.tokens[ctx.currentTokenIndex:])
		ctx.tokens[ctx.currentTokenIndex] = strings.TrimSuffix(ctx.tokens[ctx.currentTokenIndex], paren)
		ctx.tokens[ctx.currentTokenIndex+1] = paren
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

func (ctx *expressionParserContext) handleOpenParenBookKeeping() error {
	ctx.parenDepth++
	// for paren prefix, internally advance so the next getToken will not get a paren
	ctx.currentTokenIndex++
	return nil
}

func (ctx *expressionParserContext) checkAndMarkDetailedOpToken(token string) {
	if tokenIsChainOpType(token) && ctx.subCtx.lastSubFieldNode != -1 {
		// Only set chain op if there is something previously to chain
		ctx.subCtx.opTokenContext = chainOp
	} else if tokenIsCompareOpType(token) {
		ctx.subCtx.opTokenContext = compareOp
	}
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
	// For simplicity, let's not allow parenthesis without spaces
	parenMiddleRegex := regexp.MustCompile(`.+('('|')')+.+`)

	if tokenIsOpType(token) {
		ctx.checkAndMarkDetailedOpToken(token)
		return token, TokenTypeOperator, nil
	} else if valueRegex.MatchString(token) {
		// For value, strip the single quote
		token = strings.Trim(token, "'")
		return token, TokenTypeValue, nil
	} else if valueNumRegex.MatchString(token) {
		return token, TokenTypeValue, nil
	} else if parenMiddleRegex.MatchString(token) {
		return token, TokenTypeInvalid, fmt.Errorf("Error: parenthesis must have white space before or after it")
	} else if token == "true" {
		return token, TokenTypeTrue, nil
	} else if token == "false" {
		return token, TokenTypeFalse, nil
	} else if token != "(" && strings.HasPrefix(token, "(") {
		ctx.handleParenPrefix("(")
		ctx.handleOpenParenBookKeeping()
		token = ctx.tokens[ctx.currentTokenIndex]
		return token, TokenTypeParen, nil
	} else if token != "(" && strings.HasSuffix(token, "(") {
		ctx.handleParenSuffix("(")
		ctx.handleOpenParenBookKeeping()
		return ctx.getCurrentToken()
	} else if token != ")" && strings.HasSuffix(token, ")") {
		ctx.handleParenSuffix(")")
		return ctx.getCurrentToken()
	} else if token != ")" && strings.HasPrefix(token, ")") {
		ctx.handleParenPrefix(")")
		token = ctx.tokens[ctx.currentTokenIndex]
		return token, TokenTypeEndParen, ctx.handleCloseParenBookKeeping()
	} else if token == ")" {
		return token, TokenTypeEndParen, ctx.handleCloseParenBookKeeping()
	} else {
		return token, TokenTypeField, nil
	}
}

func (ctx *expressionParserContext) getErrorNeedToStartNewCtx() error {
	return ErrorNeedToStartNewCtx
}

func (ctx *expressionParserContext) getErrorNeedToStartOneNewCtx() error {
	if ctx.shortCircuitEnabled {
		// If short circuit is enabled, no need to return after one context.
		// Having recursively starting new context will make it such that the left-most
		// expression stays at the higher levels
		return ErrorNeedToStartNewCtx
	} else {
		return ErrorNeedToStartOneNewCtx
	}
}

func (ctx *expressionParserContext) checkTokenTypeWithinContext(tokenType ParseTokenType) error {
	switch ctx.subCtx.currentMode {
	case opMode:
		// opMode is pretty much a less restrictive chainMode
		fallthrough
	case chainMode:
		if tokenType == TokenTypeEndParen {
			ctx.skipAdvanceMode = true
			return NonErrorOneLayerDone
		} else if tokenType != TokenTypeOperator {
			return fmt.Errorf("Error: For operator/chain mode, token must be operator type - received %v", tokenType.String())
		} else if ctx.subCtx.currentMode != opMode && !ctx.subCtx.opTokenContext.isChainOp() {
			// This is specific for chain mode only
			return fmt.Errorf("Error: For chain mode, token must be chain type")
		}
	case fieldMode:
		// fieldMode is a more restrictive valueMode
		if tokenType == TokenTypeParen {
			return ctx.getErrorNeedToStartNewCtx()
		} else if tokenType != TokenTypeField && tokenType != TokenTypeTrue && tokenType != TokenTypeFalse && ctx.subCtx.opTokenContext.isChainOp() {
			//			return fmt.Errorf("Error: For field mode, token must be field type - received %v", tokenType.String())
			ctx.subCtx.swapFieldAndVal = true
			return ctx.getErrorNeedToStartOneNewCtx()
		}
		fallthrough
	case valueMode:
		if ctx.subCtx.swapFieldAndVal && tokenType != TokenTypeField {
			return fmt.Errorf("Error: Previously entered a value. For this value mode, token must be field type - received %v", tokenType.String())
		} else if tokenType == TokenTypeField && ctx.subCtx.opTokenContext.isChainOp() {
			return ctx.getErrorNeedToStartOneNewCtx()
		} else if tokenType == TokenTypeTrue || tokenType == TokenTypeFalse {
			if ctx.subCtx.opTokenContext.isCompareOp() {
				return fmt.Errorf("Error: Unable to do comparison operator on true or false values")
			}
		} else if tokenType != TokenTypeValue && tokenType != TokenTypeField {
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
		// FieldMode - nothing to do just record this as the last field
		ctx.subCtx.lastFieldIndex = ctx.subCtx.lastBinTreeDataNode
		ctx.subCtx.lastSubFieldNode = ctx.subCtx.lastBinTreeDataNode
	case chainMode:
		fallthrough
	case opMode:
		ctx.subCtx.lastOpIndex = ctx.subCtx.lastBinTreeDataNode
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
	// If older subctx is not used, don't merge - this means user entered an expression that started with a open paren
	if currentSubCtx.isUnused() {
		return nil
	}

	// Boundary check
	if currentSubCtx.lastOpIndex >= len(ctx.parserTree.data) {
		return ErrorNotFound
	}
	if ctx.subCtx.lastOpIndex >= len(ctx.parserTree.data) {
		return ErrorNotFound
	}

	// Note that the subContext within *ctx is considered newer spawned
	lastOpNodeOfCurrentSubCtx := &ctx.parserTree.data[currentSubCtx.lastOpIndex]
	lastOpNodeOfNewContext := &ctx.parserTree.data[ctx.subCtx.lastOpIndex]

	if lastOpNodeOfCurrentSubCtx.Right != -1 {
		return fmt.Errorf("Merging sub-context should expect value to be unassigned, but currently assigned to: %v", lastOpNodeOfCurrentSubCtx.Right)
	}
	lastOpNodeOfCurrentSubCtx.Right = ctx.subCtx.lastOpIndex

	if lastOpNodeOfNewContext.ParentIdx != -1 {
		return fmt.Errorf("Merging sub-context should expect parent to be unassigned, but currently assigned to: %v", lastOpNodeOfNewContext.ParentIdx)
	}
	lastOpNodeOfNewContext.ParentIdx = currentSubCtx.lastOpIndex

	// When merging, figure out the actual head and update it
	lastOpIndexNode := ctx.parserTree.data[currentSubCtx.lastOpIndex]
	if lastOpIndexNode.ParentIdx == -1 {
		ctx.treeHeadIndex = currentSubCtx.lastOpIndex
	}

	// Once merge is done - restore original context
	*(ctx.subCtx) = *(currentSubCtx)
	return nil
}

func (ctx *expressionParserContext) enableShortCircuitEvalIfPossible() {
	// Enable short circuit if expression is linked by only && or only ||
	var operator string
	for i := 0; i < len(ctx.tokens); i++ {
		if ctx.tokens[i] == TokenOperatorOr || ctx.tokens[i] == TokenOperatorAnd {
			if len(operator) == 0 {
				operator = ctx.tokens[i]
			} else {
				if ctx.tokens[i] != operator {
					return
				}
			}
		}
	}
	ctx.shortCircuitEnabled = true
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

func (ctx *expressionParserContext) getThisOutputNode(pos int) ParserTreeNode {
	return ctx.parserDataNodes[pos]
}

func (ctx *expressionParserContext) getLeftOutputNode(pos int) (ParserTreeNode, int) {
	thisNode := ctx.parserTree.data[pos]
	leftNode := ctx.parserDataNodes[thisNode.Left]
	return leftNode, thisNode.Left
}

func (ctx *expressionParserContext) getRightOutputNode(pos int) (ParserTreeNode, int) {
	thisNode := ctx.parserTree.data[pos]
	rightNode := ctx.parserDataNodes[thisNode.Right]
	return rightNode, thisNode.Right
}

func (ctx *expressionParserContext) outputNode(node ParserTreeNode, pos int) (Expression, error) {
	switch node.tokenType {
	case TokenTypeOperator:
		return ctx.outputOp(node, pos)
	case TokenTypeField:
		return ctx.outputField(node)
	case TokenTypeTrue:
		fallthrough
	case TokenTypeFalse:
		fallthrough
	case TokenTypeValue:
		return ctx.outputValue(node)
	default:
		return emptyExpression, fmt.Errorf("Error: Invalid Node token type: %v", node.tokenType.String())
	}
}

func (ctx *expressionParserContext) outputValue(node ParserTreeNode) (Expression, error) {
	return ValueExpr{node.data}, nil
}

func (ctx *expressionParserContext) outputField(node ParserTreeNode) (Expression, error) {
	var out FieldExpr
	var data []string
	fieldVariable := (node.data).(string)

	data, ok := ctx.varMap[fieldVariable]
	if !ok {
		// The field variable becomes the actual field in the json doc
		data = make([]string, 1)
		data[0] = fieldVariable
	}

	out.Path = data
	return out, nil
}

func (ctx *expressionParserContext) outputOp(node ParserTreeNode, pos int) (Expression, error) {
	switch (node.data).(string) {
	case TokenOperatorEqual:
		return ctx.outputEq(node, pos)
	case TokenOperatorOr:
		return ctx.outputOr(node, pos)
	case TokenOperatorAnd:
		return ctx.outputAnd(node, pos)
	case TokenOperatorGreaterThanEq:
		return ctx.outputGreaterEquals(node, pos)
	case TokenOperatorLessThan:
		return ctx.outputLessThan(node, pos)
	default:
		return emptyExpression, fmt.Errorf("Error: Invalid op type: %s", node.data)
	}
}

func (ctx *expressionParserContext) outputGreaterEquals(node ParserTreeNode, pos int) (Expression, error) {
	var out GreaterEqualExpr
	leftNode, leftPos := ctx.getLeftOutputNode(pos)
	rightNode, rightPos := ctx.getRightOutputNode(pos)

	leftSubExpr, err := ctx.outputNode(leftNode, leftPos)
	if err != nil {
		return out, err
	}

	rightSubExpr, err := ctx.outputNode(rightNode, rightPos)
	if err != nil {
		return out, err
	}

	out.Lhs = leftSubExpr
	out.Rhs = rightSubExpr

	return out, nil
}

func (ctx *expressionParserContext) outputLessThan(node ParserTreeNode, pos int) (Expression, error) {
	var out LessThanExpr
	leftNode, leftPos := ctx.getLeftOutputNode(pos)
	rightNode, rightPos := ctx.getRightOutputNode(pos)

	leftSubExpr, err := ctx.outputNode(leftNode, leftPos)
	if err != nil {
		return out, err
	}

	rightSubExpr, err := ctx.outputNode(rightNode, rightPos)
	if err != nil {
		return out, err
	}

	out.Lhs = leftSubExpr
	out.Rhs = rightSubExpr

	return out, nil
}

func (ctx *expressionParserContext) outputEq(node ParserTreeNode, pos int) (Expression, error) {
	var out EqualsExpr
	leftNode, leftPos := ctx.getLeftOutputNode(pos)
	rightNode, rightPos := ctx.getRightOutputNode(pos)

	leftSubExpr, err := ctx.outputNode(leftNode, leftPos)
	if err != nil {
		return out, err
	}

	rightSubExpr, err := ctx.outputNode(rightNode, rightPos)
	if err != nil {
		return out, err
	}

	out.Lhs = leftSubExpr
	out.Rhs = rightSubExpr

	return out, nil
}

func (ctx *expressionParserContext) outputAnd(node ParserTreeNode, pos int) (Expression, error) {
	var out AndExpr
	leftNode, leftPos := ctx.getLeftOutputNode(pos)
	rightNode, rightPos := ctx.getRightOutputNode(pos)

	leftSubExpr, err := ctx.outputNode(leftNode, leftPos)
	if err != nil {
		return out, err
	}

	rightSubExpr, err := ctx.outputNode(rightNode, rightPos)
	if err != nil {
		return out, err
	}

	out = append(out, leftSubExpr)
	out = append(out, rightSubExpr)

	return out, nil
}

func (ctx *expressionParserContext) outputOr(node ParserTreeNode, pos int) (Expression, error) {
	var out OrExpr
	leftNode, leftPos := ctx.getLeftOutputNode(pos)
	rightNode, rightPos := ctx.getRightOutputNode(pos)

	leftSubExpr, err := ctx.outputNode(leftNode, leftPos)
	if err != nil {
		return out, err
	}
	rightSubExpr, err := ctx.outputNode(rightNode, rightPos)
	if err != nil {
		return out, err
	}

	out = append(out, leftSubExpr)
	out = append(out, rightSubExpr)

	return out, nil
}

func (ctx *expressionParserContext) outputExpression() (Expression, error) {
	if ctx.parserTree.NumNodes() == 0 || ctx.treeHeadIndex == -1 {
		return emptyExpression, nil
	}

	node := ctx.getThisOutputNode(ctx.treeHeadIndex)
	if node.tokenType != TokenTypeOperator {
		return emptyExpression, fmt.Errorf("Error: Invalid op node type: %v", node.tokenType.String())
	}

	return ctx.outputOp(node, ctx.treeHeadIndex)
}

func getTokenType(token string) (ParseTokenType, error) {
	if len(token) == 0 {
		return TokenTypeInvalid, fmt.Errorf("getTokenType error: Token is of 0 length")
	} else if tokenIsOpType(token) {
		return TokenTypeOperator, nil
	}
	return TokenTypeInvalid, fmt.Errorf("getTokenType error: Unable to parse token: %v", token)
}

// Main outside facing method
func ParseSimpleExpression(strExpression string, fieldVariables FieldVarMap) (Expression, error) {
	ctx, err := NewExpressionParserCtxWithFields(strExpression, fieldVariables)
	ctx.enableShortCircuitEvalIfPossible()
	err = ctx.parse()

	if err != nil {
		return emptyExpression, err
	}

	return ctx.outputExpression()
}
