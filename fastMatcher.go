// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
)

type slotData struct {
	start int
	size  int
}

type FastMatcher struct {
	def     MatchDef
	slots   []slotData
	buckets *binTreeState
	tokens  jsonTokenizer
}

func NewFastMatcher(def *MatchDef) *FastMatcher {
	return &FastMatcher{
		def:     *def,
		slots:   make([]slotData, def.NumSlots),
		buckets: def.MatchTree.NewState(),
	}
}

func (m *FastMatcher) Reset() {
	m.slots = m.slots[:0]
	m.buckets.Reset()
}

func (m *FastMatcher) leaveValue() error {
	depth := 0

	tokens := &m.tokens
	for {
		token, _, _, err := tokens.Step()
		if err != nil {
			return err
		}

		switch token {
		case tknObjectStart:
			depth++
		case tknObjectEnd:
			if depth == 0 {
				return nil
			}
			depth--
		case tknArrayStart:
			depth++
		case tknArrayEnd:
			if depth == 0 {
				return nil
			}
			depth--
		case tknEnd:
			panic("unexpected EOF")
		}
	}
}

func (m *FastMatcher) skipValue(token tokenType) error {
	switch token {
	case tknString:
		return nil
	case tknEscString:
		return nil
	case tknInteger:
		return nil
	case tknNumber:
		return nil
	case tknNull:
		return nil
	case tknTrue:
		return nil
	case tknFalse:
		return nil
	case tknObjectStart:
		return m.leaveValue()
	case tknArrayStart:
		return m.leaveValue()
	}
	panic(fmt.Sprintf("unexpected value: %v", token))
}

func (m *FastMatcher) literalFromSlot(slot SlotID) FastVal {
	value := NewMissingFastVal()

	savePos := m.tokens.Position()

	slotInfo := m.slots[slot-1]
	m.tokens.Seek(slotInfo.start)
	token, tokenData, _, _ := m.tokens.Step()

	if isLiteralToken(token) {
		var parser fastLitParser
		value = parser.Parse(token, tokenData)
	}

	m.tokens.Seek(savePos)

	return value
}

func (m *FastMatcher) resolveFunc(fn FuncRef, activeLit *FastVal) FastVal {
	switch fn.FuncName {
	case MathFuncAbs:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathAbs(p1)
	case MathFuncAcos:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathAcos(p1)
	case MathFuncAsin:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathAsin(p1)
	case MathFuncAtan:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathAtan(p1)
	case MathFuncAtan2:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		p2 := m.resolveParam(fn.Params[1], activeLit)
		return FastValMathAtan2(p1, p2)
	case MathFuncRound:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathRound(p1)
	case MathFuncCos:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathCos(p1)
	case MathFuncSin:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathSin(p1)
	case MathFuncTan:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathTan(p1)
	case MathFuncSqrt:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathSqrt(p1)
	case MathFuncExp:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathExp(p1)
	case MathFuncLn:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathLn(p1)
	case MathFuncLog:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathLog(p1)
	case MathFuncCeil:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathCeil(p1)
	case MathFuncFloor:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathFloor(p1)
	case MathFuncDegrees:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathDegrees(p1)
	case MathFuncRadians:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathRadians(p1)
	case MathFuncPow:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		p2 := m.resolveParam(fn.Params[1], activeLit)
		return FastValMathPow(p1, p2)
	case DateFunc:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValDateFunc(p1)
	case MathFuncAdd:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		p2 := m.resolveParam(fn.Params[1], activeLit)
		return FastValMathAdd(p1, p2)
	case MathFuncSub:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		p2 := m.resolveParam(fn.Params[1], activeLit)
		return FastValMathSub(p1, p2)
	case MathFuncMul:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		p2 := m.resolveParam(fn.Params[1], activeLit)
		return FastValMathMul(p1, p2)
	case MathFuncDiv:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		p2 := m.resolveParam(fn.Params[1], activeLit)
		return FastValMathDiv(p1, p2)
	case MathFuncMod:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		p2 := m.resolveParam(fn.Params[1], activeLit)
		return FastValMathMod(p1, p2)
	case MathFuncNeg:
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathNeg(p1)
	default:
		panic(fmt.Sprintf("encountered unexpected function name: %v", fn.FuncName))
	}
}

func (m *FastMatcher) resolveParam(in interface{}, activeLit *FastVal) FastVal {
	switch opVal := in.(type) {
	case FastVal:
		return opVal
	case activeLitRef:
		if activeLit == nil {
			panic("cannot resolve active literal without having an active context")
		}

		return *activeLit
	case SlotRef:
		return m.literalFromSlot(opVal.Slot)
	case FuncRef:
		return m.resolveFunc(opVal, activeLit)
	default:
		panic(fmt.Sprintf("unexpected op value: %#v", in))
	}
}

func (m *FastMatcher) matchOp(op *OpNode, litVal *FastVal) error {
	bucketIdx := int(op.BucketIdx)

	if m.buckets.IsResolved(bucketIdx) {
		// If the bucket for this op is already resolved  in the binary tree,
		// we don't need to perform the op and can just skip it.
		return nil
	}

	lhsVal := NewMissingFastVal()
	if op.Lhs != nil {
		lhsVal = m.resolveParam(op.Lhs, litVal)
	} else if litVal != nil {
		lhsVal = *litVal
	}

	rhsVal := NewMissingFastVal()
	if op.Rhs != nil {
		rhsVal = m.resolveParam(op.Rhs, litVal)
	} else if litVal != nil {
		rhsVal = *litVal
	}

	var opRes bool
	switch op.Op {
	case OpTypeEquals:
		opRes = lhsVal.Equals(rhsVal)
	case OpTypeLessThan:
		opRes = lhsVal.Compare(rhsVal) < 0
	case OpTypeLessEquals:
		opRes = lhsVal.Compare(rhsVal) <= 0
	case OpTypeGreaterThan:
		opRes = lhsVal.Compare(rhsVal) > 0
	case OpTypeGreaterEquals:
		opRes = lhsVal.Compare(rhsVal) >= 0
	case OpTypeMatches:
		opRes = lhsVal.Matches(rhsVal)
	case OpTypeExists:
		opRes = true
	default:
		panic("invalid op type")
	}

	// Mark the result of this operation
	m.buckets.MarkNode(bucketIdx, opRes)

	// Check if running this values ops has resolved the entirety
	// of the expression, if so we can leave immediately.
	if m.buckets.IsResolved(0) {
		return nil
	}

	return nil
}

func (m *FastMatcher) matchElems(token tokenType, tokenData []byte, elems map[string]*ExecNode) error {
	// Note that this assumes that the tokenizer has already been placed at the target
	// that referenced the elements themselves...

	// Check that the token that we started with is an object that we can scan over,
	// if it is not, we need to exit early as these elements do not apply.
	if token != tknObjectStart {
		return nil
	}

	var keyLitParse fastLitParser

	for i := 0; ; i++ {
		// If this is not the first entry in the object, there should be a
		// list delimiter ('c') that shows up in the input first.
		if i != 0 {
			token, _, _, err := m.tokens.Step()
			if err != nil {
				return err
			}

			if token == tknObjectEnd {
				return nil
			}
			if token != tknListDelim {
				panic("expected object field element delimiter")
			}
		}

		token, tokenData, _, err := m.tokens.Step()
		if err != nil {
			return err
		}
		if token == tknObjectEnd {
			return nil
		}

		var keyBytes []byte
		if token == tknString {
			keyBytes = keyLitParse.ParseString(tokenData)
		} else if token == tknEscString {
			keyBytes = keyLitParse.ParseEscString(tokenData)
		} else {
			panic("expected literal")
		}

		token, _, _, err = m.tokens.Step()
		if err != nil {
			return err
		}
		if token != tknObjectKeyDelim {
			panic("expected object key delimiter")
		}

		token, tokenData, tokenDataLen, err := m.tokens.Step()
		if err != nil {
			return err
		}

		if keyElem, ok := elems[string(keyBytes)]; ok {
			// Run the execution node that applies to this particular
			// key of the object.
			m.matchExec(token, tokenData, tokenDataLen, keyElem)

			// Check if running this keys execution has resolved the entirety
			// of the expression, if so we can leave immediately.
			if m.buckets.IsResolved(0) {
				return nil
			}
		} else {
			// If we don't have any parse requirements for this key in
			// the object, we can just skip its value and continue
			m.skipValue(token)
		}
	}
}

func (m *FastMatcher) matchLoop(token tokenType, tokenData []byte, loop *LoopNode) error {
	// Note that this assumes that the tokenizer has already been placed at the target
	// that referenced the loop node itself...

	// Check that the token that we started with is an array that we can loop over,
	// if it is not, we need to exit early as this LoopNode does not apply.
	if token != tknArrayStart {
		return nil
	}

	loopBucketIdx := int(loop.BucketIdx)

	if m.buckets.IsResolved(loopBucketIdx) {
		// If the bucket for this op is already resolved  in the binary tree,
		// we don't need to perform the op and can just skip it.
		m.skipValue(token)
		return nil
	}

	// We need to keep track of the overall loop result value while the bin tree
	// is being iterated on, reset, etc...
	var loopState bool
	if loop.Mode == LoopTypeAny {
		loopState = false
	} else if loop.Mode == LoopTypeEvery {
		loopState = true
	} else if loop.Mode == LoopTypeAnyEvery {
		loopState = false
	} else {
		panic("invalid loop mode")
	}

	// We need to mark the stall index on our binary tree so that
	// resolution of a loop iteration does not propagate up the tree
	// and cause resolution of the entire expression.
	previousStallIndex := m.buckets.SetStallIndex(loopBucketIdx)

	// Scan through all the values in the loop
	for i := 0; ; i++ {
		// If this is not the first entry in the array, there should be a
		// list delimiter (',') that shows up in the input first.
		if i != 0 {
			token, _, _, err := m.tokens.Step()
			if err != nil {
				return err
			}

			if token == tknArrayEnd {
				break
			}
			if token != tknListDelim {
				panic(fmt.Sprintf("expected array element delimiter got %s", tokenToText(token)))
			}
		}

		token, tokenData, tokenDataLen, err := m.tokens.Step()
		if err != nil {
			return err
		}
		if token == tknArrayEnd {
			break
		}

		// Reset the looping node in the binary tree so that previous iterations
		// of the loop do not impact the results of this iteration
		m.buckets.ResetNode(loopBucketIdx)

		// Run the execution node for this element of the array.
		err = m.matchExec(token, tokenData, tokenDataLen, loop.Node)
		if err != nil {
			return err
		}

		iterationMatched := m.buckets.IsTrue(loopBucketIdx)
		if loop.Mode == LoopTypeAny {
			if iterationMatched {
				// If any element of the array matches, we know that
				// this loop is successful
				loopState = true

				// Skip the remainder of the array and leave the loop
				m.leaveValue()
				break
			}
		} else if loop.Mode == LoopTypeEvery {
			if !iterationMatched {
				// If any element of the array does not match, we know that
				// this loop will never match
				loopState = false

				// Skip the remainder of the array and leave the loop
				m.leaveValue()
				break
			}
		} else if loop.Mode == LoopTypeAnyEvery {
			if !iterationMatched {
				// If any element of the array does not match, we know that
				// this loop will never match the `every` semantic.
				loopState = false

				// Skip the remainder of the array and leave the loop
				m.leaveValue()
				break
			} else {
				// If we encounter a truthy value, we have satisfied the 'any'
				// semantics of this loop and should mark it as such.
				loopState = true

				// We must continue looping to satisfy the 'every' portion.
			}
		}
	}

	// We have to reset the node before we can mark it or our double-marking
	// protection on the binary tree will trigger, this helpfully also marks
	// the children of the loop to undefined resolution, which makes more sense
	// then it having the state of the last iteration of the loop.
	m.buckets.ResetNode(loopBucketIdx)

	// Reset the stall index to whatever it used to be to exit the 'context'
	// of this particular loop.  This acts as a stack in case there are
	// multiple nested loops being processed.
	m.buckets.SetStallIndex(previousStallIndex)

	// Apply the overall loop result to the binary tree
	m.buckets.MarkNode(loopBucketIdx, loopState)

	return nil
}

func (m *FastMatcher) matchAfter(node *AfterNode) error {
	savePos := m.tokens.Position()

	// Run loop matching
	for _, loop := range node.Loops {
		if slot, ok := loop.Target.(SlotRef); ok {
			slotInfo := m.slots[slot.Slot-1]

			m.tokens.Seek(slotInfo.start)
			token, tokenData, _, err := m.tokens.Step()

			// run the loop matcher
			err = m.matchLoop(token, tokenData, &loop)
			if err != nil {
				return err
			}

			if m.buckets.IsResolved(0) {
				return nil
			}
		} else {
			panic("encountered after loop with non-slot target")
		}
	}

	// Run op matching
	for _, op := range node.Ops {
		err := m.matchOp(&op, nil)
		if err != nil {
			return err
		}

		if m.buckets.IsResolved(0) {
			return nil
		}
	}

	m.tokens.Seek(savePos)

	return nil
}

func (m *FastMatcher) matchExec(token tokenType, tokenData []byte, tokenDataLen int, node *ExecNode) error {
	startPos := m.tokens.Position()
	endPos := -1

	// The start position needs to include the token we already parsed, so lets
	// back up our position based on how long that is...
	// TODO(brett19): We should probably find a more optimal way to handle this...
	startPos -= tokenDataLen

	if isLiteralToken(token) {
		var litParse fastLitParser

		// TODO(brett19): Move the litVal generation to be lazy-evaluated by the
		// op execution below so we avoid performing any translations when the op
		// is already resolved by something else.

		// Parse the literal token from the tokenizer into a FastVal value
		// to be used for op execution below.
		litVal := litParse.Parse(token, tokenData)

		for _, op := range node.Ops {
			err := m.matchOp(&op, &litVal)
			if err != nil {
				return err
			}

			if m.buckets.IsResolved(0) {
				return nil
			}
		}
	} else if token == tknObjectStart {
		objStartPos := m.tokens.Position()
		if len(node.Elems) == 0 {
			// If we have no element handlers, we can just skip the whole thing...
			m.skipValue(token)
		} else {
			err, shouldReturn := m.matchObjectOrArray(token, tokenData, node)
			if err == nil && node.After != nil {
				m.matchAfter(node.After)
			}

			if shouldReturn {
				return err
			}

			if m.buckets.IsResolved(0) {
				return nil
			}
		}
		objEndPos := m.tokens.Position()

		objFastVal := NewObjectFastVal(m.tokens.data[objStartPos:objEndPos])
		for _, op := range node.Ops {
			err := m.matchOp(&op, &objFastVal)
			if err != nil {
				return err
			}

			if m.buckets.IsResolved(0) {
				return nil
			}
		}
	} else if token == tknArrayStart {
		arrayStartPos := m.tokens.Position()
		if len(node.Loops) == 0 {
			err, shouldReturn := m.matchObjectOrArray(token, tokenData, node)
			if shouldReturn {
				return err
			}
		} else {
			// Lets save where the beginning of the array is so that for each
			// loop entry, we can easily revert back to the beginning of the
			// array to process the elements.
			savePos := m.tokens.Position()

			for loopIdx, loop := range node.Loops {
				if loop.Target != nil {
					panic("loops must always target the active state")

				}
				if loopIdx != 0 {
					// If this is not the first loop, we will need to reset back to the
					// begining of the array the loops are scanning.  In the future, perhaps
					// we can add support for parallel ExecNode handling and do it in one pass.
					m.tokens.Seek(savePos)
				}

				// Run the loop matching logic
				err := m.matchLoop(token, tokenData, &loop)
				if err != nil {
					return err
				}

				// Check if the entire expression has been resolved, if so we can simply
				// exit the entire set of looping
				if m.buckets.IsResolved(0) {
					return nil
				}
			}

		}
		arrayEndPos := m.tokens.Position()

		arrayFastVal := NewArrayFastVal(m.tokens.data[arrayStartPos:arrayEndPos])
		for _, op := range node.Ops {
			err := m.matchOp(&op, &arrayFastVal)
			if err != nil {
				return err
			}

			if m.buckets.IsResolved(0) {
				return nil
			}
		}
	} else {
		panic(fmt.Sprintf("invalid token read - tokenType: %v data: %v", token, string(tokenData)))
	}

	if node.After != nil {
		m.matchAfter(node.After)

		if m.buckets.IsResolved(0) {
			return nil
		}
	}

	endPos = m.tokens.Position()

	if node.StoreId > 0 {
		slotData := &m.slots[node.StoreId-1]
		slotData.start = startPos
		slotData.size = endPos - startPos
	}

	return nil
}

// Returns an error code, and a boolean to dictate whether or not for the caller to return immediately
func (m *FastMatcher) matchObjectOrArray(token tokenType, tokenData []byte, node *ExecNode) (error, bool) {
	var keyLitParse fastLitParser
	var endToken tokenType
	var arrayIndex int
	var arrayMode bool

	switch token {
	case tknObjectStart:
		endToken = tknObjectEnd
	case tknArrayStart:
		endToken = tknArrayEnd
		arrayMode = true
	default:
		panic("Unexpected type input for function call matchObjectOrArray")
	}

	for i := 0; ; i++ {
		// If this is not the first entry in the object, there should be a
		// list delimiter ('c') that shows up in the input first.
		if i != 0 {
			token, _, _, err := m.tokens.Step()
			if err != nil {
				return err, true
			}

			switch token {
			case tknObjectEnd:
				return nil, false
			case tknArrayEnd:
				return nil, false
			case tknEnd:
				return nil, true
			case tknListDelim:
				arrayIndex++
			// nothing
			default:
				panic(fmt.Sprintf("expected object field element delimiter, received: %v", token))
			}
		}

		token, tokenData, tokenDataLen, err := m.tokens.Step()
		if err != nil {
			return err, true
		}
		// Keep this here to catch any empty array or empty objs
		if token == endToken {
			return nil, true
		}

		// TODO(brett19): These byte-string conversion pieces are a bit wierd
		var keyString string
		var keyBytes []byte
		switch token {
		case tknString:
			keyBytes = keyLitParse.ParseStringWLen(tokenData, tokenDataLen)
		case tknEscString:
			keyBytes = keyLitParse.ParseEscStringWLen(tokenData, tokenDataLen)
		case tknArrayStart:
			// Do nothing
		case tknObjectStart:
			// Do nothing
		default:
			// If it's an array, it's possible that we're grabbing a literal like int or float, and we should not panic
			if !arrayMode {
				panic(fmt.Sprintf("expected literal, received: %v", token))
			}
		}

		if arrayMode {
			// Fake a key element by using the array index, and use the key as the actual value, tokenData
			keyString = fmt.Sprintf("[%d]", arrayIndex)
		} else {
			token, tokenData, tokenDataLen, err = m.tokens.Step()
			if err != nil {
				return err, true
			}

			if token != tknObjectKeyDelim {
				panic(fmt.Sprintf("expected object key delimiter: got %v, %v", token, string(tokenData)))
			}

			token, tokenData, tokenDataLen, err = m.tokens.Step()
			if err != nil {
				return err, true
			}
			keyString = string(keyBytes)
		}

		if keyElem, ok := node.Elems[keyString]; ok {
			// Run the execution node that applies to this particular
			// key of the object.
			m.matchExec(token, tokenData, tokenDataLen, keyElem)

			// Check if running this keys execution has resolved the entirety
			// of the expression, if so we can leave immediately.
			if m.buckets.IsResolved(0) {
				return nil, true
			}
		} else {
			// If we don't have any parse requirements for this key in
			// the object, we can just skip its value and continue
			m.skipValue(token)
		}
	}
	return nil, false
}

func (m *FastMatcher) Match(data []byte) (bool, error) {
	m.tokens.Reset(data)

	if len(data) == 0 {
		return false, nil
	}

	token, tokenData, tokenDataLen, err := m.tokens.Step()
	if err != nil {
		return false, err
	}

	err = m.matchExec(token, tokenData, tokenDataLen, m.def.ParseNode)
	if err != nil {
		return false, err
	}

	// Resolve any outstanding buckets in the tree.  This is required for
	// operators such as NOT and NEOR to correctly be resolved.
	m.buckets.Resolve()

	return m.buckets.IsTrue(0), nil
}

func (m *FastMatcher) ExpressionMatched(expressionIdx int) bool {
	binTreeIdx := m.def.MatchBuckets[expressionIdx]
	return m.buckets.IsResolved(binTreeIdx) &&
		m.buckets.IsTrue(binTreeIdx)
}
