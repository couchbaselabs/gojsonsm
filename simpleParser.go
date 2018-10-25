// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

/**
 * SimpleParser provides user to be able to specify a N1QL-Styled expression for gojsonsm.
 *
 * Values can be string or floats. Strings should be enclosed by double quotes, as to not be confused
 * with field variables
 *
 * Embedded objects are accessed via (.) accessor.
 * Example:
 * 		name.first == 'Neil'
 *
 * Field variables can be escaped by backticks ` to become literals.
 * Example:
 * 		`version0.1_serialNumber` LIKE "SN[0-9]+"
 *
 * Arrays are accessed with brackets, and integer indexes do not have to be enclosed by backticks.
 * Example:
 * 		user[10]
 * 		US.`users.ids`[10]
 *
 * Parenthesis are allowed, but must be surrounded by at least 1 white space
 * Currently, only the following operations are supported:
 * 		==/=, !=, ||/OR, &&/AND, >=, >, <=, <, LIKE/=~, NOT LIKE, EXISTS, IS MISSING, IS NULL, IS NOT NULL
 *
 * Usage example:
 * exprStr := "name.`first.name` == "Neil" && (age < 50 || isActive == true)"
 * expr, err := ParseSimpleExpression(exprStr)
 *
 * Notes:
 * - Parenthesis parsing is there but could be a bit wonky should users choose to have invalid and weird syntax with it
 */

// Values by def should be enclosed within single quotes
var valueRegex *regexp.Regexp = regexp.MustCompile(`^\".*\"$`)

// Or Values can be int or floats by themselves (w/o alpha char)
var valueNumRegex *regexp.Regexp = regexp.MustCompile(`^(-?)(0|([1-9][0-9]*))(\.[0-9]+)?$`)

var intNumRegex *regexp.Regexp = regexp.MustCompile(`^(-?)[0-9]+$`)

// Field path can be integers
var fieldTokenInt *regexp.Regexp = regexp.MustCompile(`[0-9]`)

// But they cannot have leading zeros
var fieldIndexNoLeadingZero *regexp.Regexp = regexp.MustCompile(`[^0][0-9]+`)

// Functions patterns
var funcTranslateTable map[string]string = map[string]string{
	"ABS":   MathFuncAbs,
	"ACOS":  MathFuncAcos,
	"ASIN":  MathFuncAsin,
	"ATAN":  MathFuncAtan,
	"CEIL":  MathFuncCeil,
	"COS":   MathFuncCos,
	"EXP":   MathFuncExp,
	"FLOOR": MathFuncFloor,
	"LOG":   MathFuncLog,
	"LN":    MathFuncLn,
	"SIN":   MathFuncSin,
	"TAN":   MathFuncTan,
	"ROUND": MathFuncRound,
	"SQRT":  MathFuncSqrt,
}

func getCheckFuncPattern(name string) string {
	return fmt.Sprintf(`^%s\((?P<args>.+)\)$`, name)
}

var builtInFuncCheckers map[string]*regexp.Regexp

// Support for pcre's lookahead class of regex
const lookAheadPattern = "\\(\\?\\=.+\\)"
const lookBehindPattern = "\\(\\?\\<.+\\)"
const negLookAheadPattern = "\\(\\?\\!.+\\)"
const negLookBehindPattern = "\\(\\?\\<\\!.+\\)"

var pcreCheckers [4]*regexp.Regexp = [...]*regexp.Regexp{regexp.MustCompile(lookAheadPattern),
	regexp.MustCompile(lookBehindPattern),
	regexp.MustCompile(negLookAheadPattern),
	regexp.MustCompile(negLookBehindPattern)}

type ParserTreeNode struct {
	tokenType ParseTokenType
	data      interface{}
}

func needToSpawnNewContext(err error) bool {
	return err == ErrorNeedToStartOneNewCtx || err == ErrorNeedToStartNewCtx
}

var emptyParserTreeNode ParserTreeNode

func NewParserTreeNode(tokenType ParseTokenType, data interface{}) ParserTreeNode {
	newNode := &ParserTreeNode{
		tokenType: tokenType,
		data:      data,
	}
	return *newNode
}

type parserSubContext struct {
	// Actual parser context
	currentMode            parseMode
	lastSubFieldNode       int // The last finished left side of the op
	skipAdvanceCurrentMode bool
	opTokenContext         opTokenContext
	lastMatchedFuncKey     string

	// For tree organization
	lastParserDataNode  int // Last inserted parser data node location
	lastBinTreeDataNode int // Last inserted parserTree data node location
	lastFieldIndex      int
	lastOpIndex         int
	lastValueIndex      int

	// For inserting node
	funcHelperCtx *funcOutputHelperPair

	// Means that we should return as soon as the one layer of field -> op -> value is done
	oneLayerMode bool
}

func (subctx *parserSubContext) isUnused() bool {
	return subctx.lastFieldIndex == -1 && subctx.lastOpIndex == -1 && subctx.lastValueIndex == -1 &&
		subctx.lastSubFieldNode == -1 && subctx.currentMode == fieldMode
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

type multiwordHelperPair struct {
	actualMultiWords []string
	valid            bool
}

type funcOutputHelperPair struct {
	funcName  string
	valueMode bool
	value     interface{} // if function is used in the context of a value
}

type expressionParserContext struct {
	// For token reading
	tokens               []string
	currentTokenIndex    int
	advTokenPositionOnly bool // This flag is set once, and the corresponding method will toggle it off automatically
	multiwordHelperMap   map[string]*multiwordHelperPair
	multiwordMapOnce     sync.Once
	regexCheckersInit    sync.Once
	// Split the last successful field token into subtokens for transformer's Path
	lastFieldTokens []string

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

	// For field tokens, as the parser checks the syntax of the field, it separates them into subtokens for outputting
	// to Path for field expressions. This map stores the information. Key is the index of ParserTreeNode
	fieldTokenPaths map[int][]string

	// Functions are essentially like augmented fields/values.
	// Each element in this map that is a field must have a counter part in the fieldTOkensPaths map above
	// If it's a value, then it's indicated in the pair, and the value interface is used
	funcOutputContext map[int]*funcOutputHelperPair

	// Outputting context
	currentOuputNode int

	// Compile-time determined PCRE module
	pcreWrapper PcreWrapperInterface
}

type checkFieldMode int

const (
	cfmNone          checkFieldMode = iota
	cfmBacktick      checkFieldMode = iota
	cfmNestedNumeric checkFieldMode = iota
)

func NewExpressionParserCtx(strExpression string) (*expressionParserContext, error) {
	subCtx := NewParserSubContext()
	ctx := &expressionParserContext{
		tokens:          strings.Fields(strExpression),
		subCtx:          subCtx,
		treeHeadIndex:   -1,
		fieldTokenPaths: make(map[int][]string),
	}
	return ctx, nil
}

type ParseTokenType int

const (
	TokenTypeField    ParseTokenType = iota
	TokenTypeFunc     ParseTokenType = iota
	TokenTypeOperator ParseTokenType = iota
	TokenTypeValue    ParseTokenType = iota
	TokenTypeRegex    ParseTokenType = iota
	TokenTypePcre     ParseTokenType = iota
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
	case TokenTypeRegex:
		return "TokenTypeRegex"
	case TokenTypePcre:
		return "TokenTypePcre"
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

func (ptt ParseTokenType) isBoolType() bool {
	return ptt == TokenTypeTrue || ptt == TokenTypeFalse
}

func (ptt ParseTokenType) isFieldType() bool {
	return ptt == TokenTypeField || ptt == TokenTypeFunc
}

func (ptt ParseTokenType) isOpType() bool {
	return ptt == TokenTypeOperator
}

// Regex is a type of special "value", and functions can act as values too
func (ptt ParseTokenType) isValueType() bool {
	return ptt == TokenTypeValue || ptt == TokenTypeRegex || ptt == TokenTypeFunc || ptt == TokenTypePcre
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
	TokenOperatorLike          = "=~"
	TokenOperatorExists        = "EXISTS"
)

// Other allowable operator tokens
const (
	TokenOperatorEqual2 = "="
	TokenOperatorOr2    = "OR"
	TokenOperatorAnd2   = "AND"
	TokenOperatorLike2  = "LIKE"
)

// Multi-word operator tokens
var TokenOperatorNotLike []string = []string{"NOT", "LIKE"}
var TokenOperatorIsNull []string = []string{"IS", "NULL"}
var TokenOperatorIsNotNull []string = []string{"IS", "NOT", "NULL"}
var TokenOperatorIsMissing []string = []string{"IS", "MISSING"}

// In keeping with internals, flatten it and use it as comparison for actual op when outputting
func flattenToken(token []string) string {
	return strings.Join(token, "_")
}

func replaceOpTokenIfNecessary(token string) string {
	switch token {
	case TokenOperatorEqual2:
		return TokenOperatorEqual
	case TokenOperatorOr2:
		return TokenOperatorOr
	case TokenOperatorAnd2:
		return TokenOperatorAnd
	case TokenOperatorLike2:
		return TokenOperatorLike
	}
	return token
}

func tokenIsOpType(token string) bool {
	// Equal is both numeric and logical
	return tokenIsChainOpType(token) || tokenIsEquivalentType(token) || tokenIsCompareOpType(token) || tokenIsLikeType(token) ||
		tokenIsOpOnlyType(token)
}

// This ops do not have value follow-ups
func tokenIsOpOnlyType(token string) bool {
	return tokenIsExistenceType(token) || tokenIsNullType(token)
}

func tokenIsExistenceType(token string) bool {
	return token == TokenOperatorExists || token == flattenToken(TokenOperatorIsMissing)
}

func tokenIsNullType(token string) bool {
	return token == flattenToken(TokenOperatorIsNull) || token == flattenToken(TokenOperatorIsNotNull)
}

func tokenIsLikeType(token string) bool {
	return token == TokenOperatorLike || token == TokenOperatorLike2 || token == flattenToken(TokenOperatorNotLike)
}

func tokenIsEquivalentType(token string) bool {
	return token == TokenOperatorEqual || token == TokenOperatorEqual2 || token == TokenOperatorNotEqual
}

// Comparison Operator can be used for both string comparison and numeric
func tokenIsCompareOpType(token string) bool {
	return token == TokenOperatorGreaterThanEq || token == TokenOperatorLessThan || token == TokenOperatorLessThanEq || token == TokenOperatorGreaterThan
}

// Chain-op are operators that can chain multiple expressions together
func tokenIsChainOpType(token string) bool {
	return token == TokenOperatorAnd || token == TokenOperatorAnd2 || token == TokenOperatorOr || token == TokenOperatorOr2
}

func (ctx *expressionParserContext) tokenIsBuiltInFuncType(token string) bool {
	ctx.regexCheckersInit.Do(func() {
		builtInFuncCheckers = make(map[string]*regexp.Regexp)
		ctx.funcOutputContext = make(map[int]*funcOutputHelperPair)
		for k, _ := range funcTranslateTable {
			regex := regexp.MustCompile(getCheckFuncPattern(k))
			builtInFuncCheckers[k] = regex
		}
	})

	ctx.subCtx.lastMatchedFuncKey = ""
	for key, checker := range builtInFuncCheckers {
		if checker.MatchString(token) {
			ctx.subCtx.lastMatchedFuncKey = key
			return true
		}
	}
	return false
}

// Returns true if the value is to be used for pcre types
func tokenIsPcreValueType(token string) bool {
	for _, pcreChecker := range pcreCheckers {
		if pcreChecker.MatchString(token) {
			return true
		}
	}
	return false
}

func (opCtx opTokenContext) isChainOp() bool {
	return opCtx == chainOp
}

func (opCtx opTokenContext) isCompareOp() bool {
	return opCtx == compareOp
}

func (opCtx opTokenContext) isLikeOp() bool {
	return opCtx == matchOp
}

func (opCtx *opTokenContext) clear() {
	if *opCtx != noOp {
		*opCtx = noOp
	}
}

func (ctx *expressionParserContext) advanceToken() error {
	ctx.currentTokenIndex++

	if ctx.advTokenPositionOnly {
		ctx.advTokenPositionOnly = false
		return nil
	}

	ctx.subCtx.lastMatchedFuncKey = ""
	ctx.subCtx.funcHelperCtx = nil

	// context mode transition
	switch ctx.subCtx.currentMode {
	case fieldMode:
		// After the field mode, the next token *must* be an op
		ctx.subCtx.currentMode = opMode
	case opMode:
		switch ctx.subCtx.opTokenContext {
		// These ops do not have fields
		case noFieldOp:
			ctx.subCtx.currentMode = chainMode
		default:
			// After the op mode, the next mode should be value mode
			ctx.subCtx.currentMode = valueMode
		}
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
	case chainMode:
		ctx.subCtx.currentMode = fieldMode
	default:
		return fmt.Errorf("Not implemented yet for mode transition %v", ctx.subCtx.currentMode)
	}
	return nil
}

func (ctx *expressionParserContext) handleParenPrefix(paren string) error {
	// Strip the "(" or ")" from this token and into its own element
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
	ctx.advTokenPositionOnly = true
	return ctx.advanceToken()
}

// Given a specific op token, set the opTokenContext to the type of op it is
func (ctx *expressionParserContext) checkAndMarkDetailedOpToken(token string) {
	if tokenIsChainOpType(token) && ctx.subCtx.lastSubFieldNode != -1 {
		// Only set chain op if there is something previously to chain
		ctx.subCtx.opTokenContext = chainOp
	} else if tokenIsCompareOpType(token) {
		ctx.subCtx.opTokenContext = compareOp
	} else if tokenIsLikeType(token) {
		ctx.subCtx.opTokenContext = matchOp
	} else if tokenIsOpOnlyType(token) {
		ctx.subCtx.opTokenContext = noFieldOp
	}
}

func (ctx *expressionParserContext) checkIfTokenIsPotentiallyOpType(token string) bool {
	if token == TokenOperatorNotLike[0] || token == TokenOperatorIsNull[0] || token == TokenOperatorIsNotNull[0] {
		ctx.multiwordMapOnce.Do(func() {
			ctx.multiwordHelperMap = make(map[string]*multiwordHelperPair)
			ctx.multiwordHelperMap[flattenToken(TokenOperatorNotLike)] = &multiwordHelperPair{
				actualMultiWords: TokenOperatorNotLike,
			}
			ctx.multiwordHelperMap[flattenToken(TokenOperatorIsNull)] = &multiwordHelperPair{
				actualMultiWords: TokenOperatorIsNull,
			}
			ctx.multiwordHelperMap[flattenToken(TokenOperatorIsNotNull)] = &multiwordHelperPair{
				actualMultiWords: TokenOperatorIsNotNull,
			}
			ctx.multiwordHelperMap[flattenToken(TokenOperatorIsMissing)] = &multiwordHelperPair{
				actualMultiWords: TokenOperatorIsMissing,
			}
		})
		for _, v := range ctx.multiwordHelperMap {
			v.valid = true
		}
		return true
	}
	return false
}

func (ctx *expressionParserContext) handleMultiTokens() (string, ParseTokenType, error) {
	var tokenOrig string = ctx.tokens[ctx.currentTokenIndex]
	numValids := len(ctx.multiwordHelperMap)

outerLoop:
	for i := 0; ctx.currentTokenIndex+i < len(ctx.tokens); i++ {
		token := ctx.tokens[ctx.currentTokenIndex+i]
		retry := true
		for retry {
			retry = false
			for fstr, pair := range ctx.multiwordHelperMap {
				if !pair.valid {
					continue
				}
				if i >= len(pair.actualMultiWords) || pair.actualMultiWords[i] != token {
					pair.valid = false
					numValids--
					retry = true
					break
				}
				if numValids == 0 {
					break outerLoop
				} else if numValids == 1 && i == len(pair.actualMultiWords)-1 && pair.actualMultiWords[i] == token {
					ctx.currentTokenIndex += i
					ctx.checkAndMarkDetailedOpToken(fstr)
					return fstr, TokenTypeOperator, nil
				}
			}
		}
	}

	return tokenOrig, TokenTypeInvalid, fmt.Errorf("Error: Invalid use of keyword for token: %s", tokenOrig)
}

func (ctx *expressionParserContext) getCurrentTokenParenHelper(token string) (string, ParseTokenType, error) {
	// For simplicity, let's not allow parenthesis without spaces
	parenMiddleRegex := regexp.MustCompile(`[A-Za-z]+(\(|\))+[A-Za-z]+`)

	if token != "(" && strings.HasPrefix(token, "(") {
		ctx.handleParenPrefix("(")
		return ctx.getCurrentToken()
	} else if token != "(" && strings.HasSuffix(token, "(") {
		ctx.handleParenSuffix("(")
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
	} else if token == "(" {
		return token, TokenTypeParen, ctx.handleOpenParenBookKeeping()
	} else if parenMiddleRegex.MatchString(token) {
		return token, TokenTypeInvalid, ErrorParenWSpace
	}

	return token, TokenTypeInvalid, fmt.Errorf("Invalid parenthesis case")
}

func (ctx *expressionParserContext) getTokenValueSubtype() ParseTokenType {
	if ctx.subCtx.opTokenContext.isLikeOp() {
		return TokenTypeRegex
	} else {
		return TokenTypeValue
	}
}

func (ctx *expressionParserContext) getValueTokenHelper() (string, ParseTokenType, error) {
	token := ctx.tokens[ctx.currentTokenIndex]

	// For value, strip the double quotes
	token = strings.Trim(token, "\"")

	if ctx.getTokenValueSubtype() != TokenTypeValue {
		_, err := regexp.Compile(token)
		if err != nil {
			if tokenIsPcreValueType(token) {
				return token, TokenTypePcre, nil
			}
			return token, TokenTypeRegex, err
		}
	}

	return token, ctx.getTokenValueSubtype(), nil
}

func (ctx *expressionParserContext) getCurrentToken() (string, ParseTokenType, error) {
	if ctx.currentTokenIndex >= len(ctx.tokens) {
		return "", TokenTypeInvalid, ErrorNoMoreTokens
	}

	token := ctx.tokens[ctx.currentTokenIndex]
	if ctx.checkIfTokenIsPotentiallyOpType(token) {
		return ctx.handleMultiTokens()
	} else if tokenIsOpType(token) {
		token = replaceOpTokenIfNecessary(token)
		ctx.checkAndMarkDetailedOpToken(token)
		return token, TokenTypeOperator, nil
	} else if valueRegex.MatchString(token) {
		return ctx.getValueTokenHelper()
	} else if valueNumRegex.MatchString(token) {
		return token, ctx.getTokenValueSubtype(), nil
	} else if token == "true" {
		return token, TokenTypeTrue, nil
	} else if token == "false" {
		return token, TokenTypeFalse, nil
	} else if ctx.tokenIsBuiltInFuncType(token) {
		return ctx.getFuncFieldTokenHelper(token)
	} else if strings.Contains(token, "(") || strings.Contains(token, ")") {
		return ctx.getCurrentTokenParenHelper(token)
	} else if tokenIsUnfinishedValueType(token) {
		return ctx.getUnfinishedValueHelper(`"`)
	} else {
		return ctx.getTokenFieldTokenHelper(token)
	}
}

func tokenIsUnfinishedValueType(token string) bool {
	return strings.HasPrefix(token, `"`) && !strings.HasSuffix(token, `"`)
}

func (ctx *expressionParserContext) getUnfinishedValueHelper(delim string) (string, ParseTokenType, error) {
	outputToken := strings.Trim(ctx.tokens[ctx.currentTokenIndex], delim)
	tokensLen := len(ctx.tokens)
	for ctx.currentTokenIndex++; ctx.currentTokenIndex < tokensLen; ctx.currentTokenIndex++ {
		var breakout bool
		token := ctx.tokens[ctx.currentTokenIndex]
		if ctx.parenDepth > 0 && strings.HasSuffix(token, ")") {
			ctx.handleParenSuffix(")")
			tokensLen = len(ctx.tokens)
			token = ctx.tokens[ctx.currentTokenIndex]
		}
		if strings.HasSuffix(token, delim) {
			breakout = true
			token = strings.Trim(token, delim)
		}
		outputToken = fmt.Sprintf("%s %s", outputToken, token)

		if breakout {
			break
		}
	}

	if ctx.currentTokenIndex == tokensLen {
		return "", TokenTypeInvalid, ErrorMissingQuote
	}

	return outputToken, ctx.getTokenValueSubtype(), nil
}

func (ctx *expressionParserContext) getFuncFieldTokenHelper(token string) (string, ParseTokenType, error) {
	regex := builtInFuncCheckers[ctx.subCtx.lastMatchedFuncKey]

	subMatches := regex.FindStringSubmatch(token)
	if len(subMatches) != 2 {
		return token, TokenTypeFunc, ErrorInvalidFuncArgs
	}
	args := subMatches[1]

	if ctx.subCtx.currentMode == fieldMode {
		ctx.lastFieldTokens = make([]string, 0)
		ctx.subCtx.funcHelperCtx = &funcOutputHelperPair{funcName: funcTranslateTable[ctx.subCtx.lastMatchedFuncKey]}
		return token, TokenTypeFunc, checkAndParseField(args, &ctx.lastFieldTokens)
	} else if ctx.subCtx.currentMode == valueMode {
		ctx.subCtx.funcHelperCtx = &funcOutputHelperPair{funcName: funcTranslateTable[ctx.subCtx.lastMatchedFuncKey],
			valueMode: true,
		}
		// For value, strip the double quotes
		args = strings.Trim(args, "\"")
		if valueNumRegex.MatchString(args) {
			numArg, err := strconv.Atoi(args)
			if err != nil {
				return token, TokenTypeFunc, fmt.Errorf("Error converting argument when parsing func argument: %v", err)
			}
			ctx.subCtx.funcHelperCtx.value = numArg
		} else {
			ctx.subCtx.funcHelperCtx.value = args
		}
		return token, TokenTypeFunc, nil
	} else {
		return token, TokenTypeFunc, fmt.Errorf("Error: %v mode is invalid for functions", ctx.subCtx.currentMode.String())
	}
}

// Checks the syntax of field - i.e. paths, array syntax, etc
func (ctx *expressionParserContext) getTokenFieldTokenHelper(token string) (string, ParseTokenType, error) {
	var err error

	// Field name cannot start or end with a period
	invalidPeriodPosRegex := regexp.MustCompile(`(^\.)|(\.$)`)
	if invalidPeriodPosRegex.MatchString(token) {
		err = fmt.Errorf("Invalid field: %v - cannot start or end with a period", token)
	}

	if err != nil {
		return token, TokenTypeField, err
	}

	ctx.lastFieldTokens = make([]string, 0)

	return token, TokenTypeField, checkAndParseField(token, &ctx.lastFieldTokens)
}

func checkAndParseField(token string, subTokens *[]string) error {
	var pos int
	var beginPos int
	var mode checkFieldMode
	var nextMode checkFieldMode
	var skipAppend bool

	if len(token) == 0 {
		return ErrorEmptyToken
	}

	for ; pos < len(token); pos++ {
		switch mode {
		case cfmNone:
			switch string(token[pos]) {
			case fieldSeparator:
				if !skipAppend {
					*subTokens = append(*subTokens, string(token[beginPos:pos]))
				} else {
					skipAppend = false
				}
				beginPos = pos + 1
			case fieldLiteral:
				mode = cfmBacktick
				beginPos = pos + 1
				nextMode = cfmNone
			case fieldNestedStart:
				if !skipAppend {
					*subTokens = append(*subTokens, string(token[beginPos:pos]))
				} else {
					skipAppend = false
				}
				beginPos = pos
				mode = cfmNestedNumeric
			}
		case cfmBacktick:
			// Keep going until we find another literal seperator
			switch string(token[pos]) {
			case fieldLiteral:
				if beginPos == pos {
					return ErrorEmptyLiteral
				}
				*subTokens = append(*subTokens, string(token[beginPos:pos]))
				mode = nextMode
				if pos != len(token)-1 && (string(token[pos+1]) == fieldSeparator || string(token[pos+1]) == fieldNestedStart) || pos == len(token)-1 {
					skipAppend = true
				}
			}
		case cfmNestedNumeric:
			if pos == beginPos {
				continue
			}
			if pos == beginPos+1 && string(token[pos]) == "0" {
				return ErrorLeadingZeroes
			} else if !fieldTokenInt.MatchString(string(token[pos])) && string(token[pos]) != fieldNestedEnd {
				return ErrorAllInts
			}
			switch string(token[pos]) {
			case fieldNestedEnd:
				// If nothing was entered between the brackets
				if pos == beginPos+1 {
					return ErrorEmptyNest
				}

				// Advance mode to the next, and skip appending if this is the last or is followed by another nest
				mode = cfmNone
				if !skipAppend {
					*subTokens = append(*subTokens, token[beginPos:pos+1])
				} else {
					skipAppend = false
				}
				if pos == len(token)-1 || (pos < len(token)-1 && string(token[pos+1]) == fieldNestedStart) {
					skipAppend = true
				}
			case fieldSeparator:
				fallthrough
			case fieldLiteral:
				// For now, bracket can be used only for array indexing
				return ErrorAllInts
			}
		}
	}

	// Catch any outstanding mismatched backticks or anything else
	switch mode {
	case cfmNone:
		if !skipAppend {
			// Capture the last string, whatever it is
			*subTokens = append(*subTokens, string(token[beginPos:pos]))
		}
	case cfmNestedNumeric:
		fallthrough
	case cfmBacktick:
		return ErrorMissingBacktickBracket
	}

	return nil
}

func (ctx *expressionParserContext) getErrorNeedToStartNewCtx() error {
	return ErrorNeedToStartNewCtx
}

func (ctx *expressionParserContext) getErrorNeedToStartOneNewCtx() error {
	if ctx.shortCircuitEnabled {
		// If short circuit is enabled, no need to return after one context.
		// Having recursively starting new context will make it such that the left-most
		// expression stays at the higher levels to be evaluated first
		return ErrorNeedToStartNewCtx
	} else {
		return ErrorNeedToStartOneNewCtx
	}
}

func (ctx *expressionParserContext) checkTokenTypeWithinContext(tokenType ParseTokenType, token string) error {
	switch ctx.subCtx.currentMode {
	case opMode:
		// opMode is pretty much a less restrictive chainMode
		fallthrough
	case chainMode:
		if tokenType == TokenTypeEndParen {
			// For end parenthesis, do not advance the context
			ctx.advTokenPositionOnly = true
			return NonErrorOneLayerDone
		} else if !tokenType.isOpType() {
			return fmt.Errorf("Error: For operator/chain mode, token must be operator type - received %v(%v)", token, tokenType.String())
		} else if ctx.subCtx.currentMode != opMode && !ctx.subCtx.opTokenContext.isChainOp() {
			// This is specific for chain mode only
			return fmt.Errorf("Error: For chain mode, token must be chain type - received %v(%v)", token, tokenType.String())
		}
	case fieldMode:
		// fieldMode is a more restrictive valueMode
		if tokenType == TokenTypeParen {
			return ctx.getErrorNeedToStartNewCtx()
		} else if !tokenType.isFieldType() && !tokenType.isBoolType() {
			return fmt.Errorf("Error: For field mode, expecting a field type. Received: %v(%v)", token, tokenType)
		}
		fallthrough
	case valueMode:
		if tokenType.isFieldType() && ctx.subCtx.opTokenContext.isChainOp() {
			return ctx.getErrorNeedToStartOneNewCtx()
		} else if tokenType.isBoolType() {
			if ctx.subCtx.opTokenContext.isCompareOp() || ctx.subCtx.opTokenContext.isLikeOp() {
				return fmt.Errorf("Error: Unable to do comparison operator on true or false values")
			}
		} else if !tokenType.isValueType() && !tokenType.isFieldType() {
			return fmt.Errorf("Error: For value mode, token must be value type - received %v(%v)", token, tokenType.String())
		}
	default:
		return fmt.Errorf("Error: Not implemented for tokenType: %v(%v)", token, tokenType.String())
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
		// Store the corresponding path vaiables
		ctx.fieldTokenPaths[ctx.subCtx.lastBinTreeDataNode] = DeepCopyStringArray(ctx.lastFieldTokens)

		if ctx.subCtx.funcHelperCtx != nil {
			ctx.funcOutputContext[ctx.subCtx.lastFieldIndex] = ctx.subCtx.funcHelperCtx
		}
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

		// Now that we have completed a sub-expression, the "op" becomes the parent of the last completed nodes
		ctx.subCtx.lastSubFieldNode = ctx.subCtx.lastOpIndex

		if ctx.subCtx.funcHelperCtx != nil {
			ctx.funcOutputContext[ctx.subCtx.lastValueIndex] = ctx.subCtx.funcHelperCtx
		}
	default:
		// OK to leak the node created above because we should be erroring out anyway
		return fmt.Errorf("Unsure how to insert into tree: %v", newNode)
	}

	return nil
}

// Given two sub contexts, one in the ctx, and the older one, merge them
func (ctx *expressionParserContext) mergeAndRestoreSubContexts(olderSubCtx *parserSubContext) error {
	// If older subctx is not used, don't merge (nor restore orig context)
	// - this means user entered an expression that started with a open paren
	if olderSubCtx.isUnused() {
		return nil
	}

	// Boundary check
	if olderSubCtx.lastOpIndex >= len(ctx.parserTree.data) {
		return ErrorNotFound
	}
	if ctx.subCtx.lastOpIndex >= len(ctx.parserTree.data) {
		return ErrorNotFound
	}

	// Note that the subContext within *ctx is considered newer spawned
	lastOpNodeOfOlderSubCtx := &ctx.parserTree.data[olderSubCtx.lastOpIndex]
	lastOpNodeOfNewContext := &ctx.parserTree.data[ctx.subCtx.lastOpIndex]

	if lastOpNodeOfOlderSubCtx.Right != -1 {
		return fmt.Errorf("Merging sub-context should expect value to be unassigned, but currently assigned to: %v", lastOpNodeOfOlderSubCtx.Right)
	}
	lastOpNodeOfOlderSubCtx.Right = ctx.subCtx.lastOpIndex

	if lastOpNodeOfNewContext.ParentIdx != -1 {
		return fmt.Errorf("Merging sub-context should expect parent to be unassigned, but currently assigned to: %v", lastOpNodeOfNewContext.ParentIdx)
	}
	lastOpNodeOfNewContext.ParentIdx = olderSubCtx.lastOpIndex

	// When merging, figure out the actual head and update it - the op that no longer merges to a parent is to be the new head
	lastOpIndexNode := ctx.parserTree.data[olderSubCtx.lastOpIndex]
	if lastOpIndexNode.ParentIdx == -1 {
		ctx.treeHeadIndex = olderSubCtx.lastOpIndex
	}

	// Once merge is done - restore original context
	*(ctx.subCtx) = *(olderSubCtx)
	return nil
}

// Enable short circuit if expression is linked by only && or only ||
func (ctx *expressionParserContext) enableShortCircuitEvalIfPossible() {
	var operator string
	for i := 0; i < len(ctx.tokens); i++ {
		if ctx.tokens[i] == TokenOperatorOr || ctx.tokens[i] == TokenOperatorAnd {
			if len(operator) == 0 {
				ctx.shortCircuitEnabled = true
				operator = ctx.tokens[i]
			} else {
				if ctx.tokens[i] != operator {
					ctx.shortCircuitEnabled = false
					return
				}
			}
		}
	}
}

// Main high level portion of the parser
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
		err = ctx.checkTokenTypeWithinContext(tokenType, token)
		if err == nil {
			// Push the token into the correct location into the tree
			err = ctx.insertNode(NewParserTreeNode(tokenType, token))
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

			// Recursively fill in the necessary information in the newer subcontext
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
		err = nil
		if ctx.parenDepth > 0 {
			err = ErrorParenMismatch
		}
	}
	return err
}

// Output helpers - return the actual node with data (and optionally the index position of the sought after node)
func (ctx *expressionParserContext) getThisOutputNode(pos int) ParserTreeNode {
	if pos >= len(ctx.parserDataNodes) {
		return emptyParserTreeNode
	}
	return ctx.parserDataNodes[pos]
}

func (ctx *expressionParserContext) getLeftOutputNode(pos int) (ParserTreeNode, int) {
	if pos >= len(ctx.parserTree.data) {
		return emptyParserTreeNode, -1
	}
	thisNode := ctx.parserTree.data[pos]

	if thisNode.Left >= len(ctx.parserDataNodes) {
		return emptyParserTreeNode, -1
	}
	leftNode := ctx.parserDataNodes[thisNode.Left]
	return leftNode, thisNode.Left
}

func (ctx *expressionParserContext) getRightOutputNode(pos int) (ParserTreeNode, int) {
	if pos >= len(ctx.parserTree.data) {
		return emptyParserTreeNode, -1
	}
	thisNode := ctx.parserTree.data[pos]

	if thisNode.Left >= len(ctx.parserDataNodes) {
		return emptyParserTreeNode, -1
	}
	rightNode := ctx.parserDataNodes[thisNode.Right]
	return rightNode, thisNode.Right
}

// Main function used during output to funnel to the right type of output method
func (ctx *expressionParserContext) outputNode(node ParserTreeNode, pos int) (Expression, error) {
	if node == emptyParserTreeNode || pos == -1 {
		return emptyExpression, fmt.Errorf("Error: Unable to parse internal tree data structure")
	}

	switch node.tokenType {
	case TokenTypeOperator:
		return ctx.outputOp(node, pos)
	case TokenTypeField:
		return ctx.outputField(pos)
	case TokenTypeTrue:
		return ctx.outputTrue()
	case TokenTypeFalse:
		return ctx.outputFalse()
	case TokenTypeValue:
		return ctx.outputValue(node)
	case TokenTypeRegex:
		return ctx.outputRegex(node)
	case TokenTypeFunc:
		return ctx.outputFunc(pos)
	case TokenTypePcre:
		return ctx.outputPcre(node)
	default:
		return emptyExpression, fmt.Errorf("Error: Invalid Node token type: %v", node.tokenType.String())
	}
}

func (ctx *expressionParserContext) outputTrue() (Expression, error) {
	return ValueExpr{true}, nil
}

func (ctx *expressionParserContext) outputFalse() (Expression, error) {
	return ValueExpr{false}, nil
}

func (ctx *expressionParserContext) outputValue(node ParserTreeNode) (Expression, error) {
	var outputData interface{} = node.data
	var err error
	if strData, ok := node.data.(string); ok && valueNumRegex.MatchString(strData) {
		if intNumRegex.MatchString(strData) {
			outputData, err = strconv.ParseInt(strData, 10, 64)
		} else {
			outputData, err = strconv.ParseFloat(strData, 64)
		}
	}
	return ValueExpr{outputData}, err
}

func (ctx *expressionParserContext) outputRegex(node ParserTreeNode) (Expression, error) {
	return RegexExpr{node.data}, nil
}

func (ctx *expressionParserContext) outputPcre(node ParserTreeNode) (Expression, error) {
	return PcreExpr{node.data}, nil
}

func (ctx *expressionParserContext) outputField(pos int) (Expression, error) {
	var out FieldExpr
	path, ok := ctx.fieldTokenPaths[pos]
	if !ok {
		return out, fmt.Errorf("Error: Unable to find internally stored field path")
	}

	out.Path = path
	return out, nil
}

func (ctx *expressionParserContext) outputFunc(pos int) (Expression, error) {
	var out FuncExpr

	pair, ok := ctx.funcOutputContext[pos]
	if !ok {
		return out, fmt.Errorf("Error: Unable to find internally stored function name for outputting")
	}
	out.FuncName = pair.funcName

	var subFieldExpr Expression
	var err error
	if !pair.valueMode {
		subFieldExpr, err = ctx.outputField(pos)
	} else {
		subFieldExpr, err = ctx.outputValue(NewParserTreeNode(TokenTypeValue, pair.value))
	}
	out.Params = append(out.Params, subFieldExpr)
	return out, err
}

func (ctx *expressionParserContext) outputOp(node ParserTreeNode, pos int) (Expression, error) {
	nodeData, ok := (node.data).(string)
	if !ok || pos == -1 {
		return emptyExpression, fmt.Errorf("Unable to parse internal tree data structure")
	}

	switch nodeData {
	case TokenOperatorEqual:
		return ctx.outputEq(node, pos)
	case TokenOperatorNotEqual:
		return ctx.outputNotEq(node, pos)
	case TokenOperatorOr:
		return ctx.outputOr(node, pos)
	case TokenOperatorAnd:
		return ctx.outputAnd(node, pos)
	case TokenOperatorLessThan:
		return ctx.outputLessThan(node, pos)
	case TokenOperatorLessThanEq:
		return ctx.outputLessThanEq(node, pos)
	case TokenOperatorGreaterThan:
		return ctx.outputGreaterThan(node, pos)
	case TokenOperatorGreaterThanEq:
		return ctx.outputGreaterThanEq(node, pos)
	case TokenOperatorLike:
		return ctx.outputLike(node, pos)
	case flattenToken(TokenOperatorNotLike):
		return ctx.outputNotLike(node, pos)
	case TokenOperatorExists:
		return ctx.outputExists(node, pos)
	case flattenToken(TokenOperatorIsMissing):
		return ctx.outputIsMissing(node, pos)
	case flattenToken(TokenOperatorIsNull):
		return ctx.outputIsNull(node, pos)
	case flattenToken(TokenOperatorIsNotNull):
		return ctx.outputIsNotNull(node, pos)
	default:
		return emptyExpression, fmt.Errorf("Error: Invalid op type: %s", nodeData)
	}
}

func (ctx *expressionParserContext) getComparisonSubExprsNodes(node ParserTreeNode, pos int) (Expression, Expression, error) {
	leftNode, leftPos := ctx.getLeftOutputNode(pos)
	rightNode, rightPos := ctx.getRightOutputNode(pos)

	leftSubExpr, err := ctx.outputNode(leftNode, leftPos)
	if err != nil {
		return nil, nil, err
	}

	rightSubExpr, err := ctx.outputNode(rightNode, rightPos)
	if err != nil {
		return nil, nil, err
	}

	return leftSubExpr, rightSubExpr, err
}

func (ctx *expressionParserContext) getSingleLeftSubExprsNodes(node ParserTreeNode, pos int) (Expression, error) {
	leftNode, leftPos := ctx.getLeftOutputNode(pos)

	leftSubExpr, err := ctx.outputNode(leftNode, leftPos)
	if err != nil {
		return nil, err
	}

	return leftSubExpr, err
}

func (ctx *expressionParserContext) outputEq(node ParserTreeNode, pos int) (Expression, error) {
	leftSubExpr, rightSubExpr, err := ctx.getComparisonSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return EqualsExpr{
		leftSubExpr,
		rightSubExpr,
	}, nil
}

func (ctx *expressionParserContext) outputNotEq(node ParserTreeNode, pos int) (Expression, error) {
	leftSubExpr, rightSubExpr, err := ctx.getComparisonSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return NotEqualsExpr{
		leftSubExpr,
		rightSubExpr,
	}, nil
}

func (ctx *expressionParserContext) outputLessThan(node ParserTreeNode, pos int) (Expression, error) {
	leftSubExpr, rightSubExpr, err := ctx.getComparisonSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return LessThanExpr{
		leftSubExpr,
		rightSubExpr,
	}, nil
}

func (ctx *expressionParserContext) outputLessThanEq(node ParserTreeNode, pos int) (Expression, error) {
	leftSubExpr, rightSubExpr, err := ctx.getComparisonSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return LessEqualsExpr{
		leftSubExpr,
		rightSubExpr,
	}, nil
}

func (ctx *expressionParserContext) outputGreaterThan(node ParserTreeNode, pos int) (Expression, error) {
	leftSubExpr, rightSubExpr, err := ctx.getComparisonSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return GreaterThanExpr{
		leftSubExpr,
		rightSubExpr,
	}, nil
}

func (ctx *expressionParserContext) outputGreaterThanEq(node ParserTreeNode, pos int) (Expression, error) {
	leftSubExpr, rightSubExpr, err := ctx.getComparisonSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return GreaterEqualsExpr{
		leftSubExpr,
		rightSubExpr,
	}, nil
}

func (ctx *expressionParserContext) outputLike(node ParserTreeNode, pos int) (Expression, error) {
	leftSubExpr, rightSubExpr, err := ctx.getComparisonSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return LikeExpr{
		leftSubExpr,
		rightSubExpr,
	}, nil
}

func (ctx *expressionParserContext) outputNotLike(node ParserTreeNode, pos int) (Expression, error) {
	matchExpr, err := ctx.outputLike(node, pos)
	if err != nil {
		return nil, err
	}

	return NotExpr{
		matchExpr,
	}, nil
}

func (ctx *expressionParserContext) outputExists(node ParserTreeNode, pos int) (Expression, error) {
	subExpr, err := ctx.getSingleLeftSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return ExistsExpr{
		subExpr,
	}, nil
}

func (ctx *expressionParserContext) outputIsMissing(node ParserTreeNode, pos int) (Expression, error) {
	subExpr, err := ctx.getSingleLeftSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return NotExistsExpr{
		subExpr,
	}, nil
}

func (ctx *expressionParserContext) outputIsNull(node ParserTreeNode, pos int) (Expression, error) {
	subExpr, err := ctx.getSingleLeftSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return EqualsExpr{
		subExpr,
		ValueExpr{nil},
	}, nil
}

func (ctx *expressionParserContext) outputIsNotNull(node ParserTreeNode, pos int) (Expression, error) {
	subExpr, err := ctx.getSingleLeftSubExprsNodes(node, pos)
	if err != nil {
		return nil, err
	}

	return NotEqualsExpr{
		subExpr,
		ValueExpr{nil},
	}, nil
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

// Main output function to retrieve an Expression from the internal data of expressionParserContext
func (ctx *expressionParserContext) outputExpression() (Expression, error) {
	if ctx.parserTree.NumNodes() == 0 || ctx.treeHeadIndex == -1 {
		return emptyExpression, fmt.Errorf("Error: string expression has not been parsed into internal data structures")
	}

	node := ctx.getThisOutputNode(ctx.treeHeadIndex)
	if node == emptyParserTreeNode {
		return emptyExpression, fmt.Errorf("Error: Incorrectly parsed context")
	}

	if node.tokenType != TokenTypeOperator {
		return emptyExpression, fmt.Errorf("Error: Invalid op node type: %v", node.tokenType.String())
	}

	return ctx.outputOp(node, ctx.treeHeadIndex)
}

// MAIN
func ParseSimpleExpression(strExpression string) (Expression, error) {
	ctx, err := NewExpressionParserCtx(strExpression)
	ctx.enableShortCircuitEvalIfPossible()
	err = ctx.parse()

	if err != nil {
		return emptyExpression, err
	}

	return ctx.outputExpression()
}
