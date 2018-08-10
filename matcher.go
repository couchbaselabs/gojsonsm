// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
)

type slotData struct {
	start int
	size  int
}

type Matcher struct {
	def     MatchDef
	slots   []slotData
	buckets *binTreeState
	tokens  jsonTokenizer
}

func NewMatcher(def *MatchDef) *Matcher {
	return &Matcher{
		def:     *def,
		slots:   make([]slotData, def.NumSlots),
		buckets: def.MatchTree.NewState(),
	}
}

func (m *Matcher) Reset() {
	m.slots = m.slots[:0]
	m.buckets.Reset()
}

func (m *Matcher) leaveValue() error {
	depth := 0

	tokens := &m.tokens
	for {
		token, _, err := tokens.Step()
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

func (m *Matcher) skipValue(token tokenType) error {
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
	panic("unexpected value")
}

func (m *Matcher) literalFromSlot(slot SlotID) FastVal {
	value := NewMissingFastVal()

	savePos := m.tokens.Position()

	slotInfo := m.slots[slot-1]
	m.tokens.Seek(slotInfo.start)
	token, tokenData, _ := m.tokens.Step()

	if isLiteralToken(token) {
		var parser fastLitParser
		value = parser.Parse(token, tokenData)
	}

	m.tokens.Seek(savePos)

	return value
}

func (m *Matcher) resolveFunc(fn FuncRef, activeLit *FastVal) FastVal {
	switch fn.FuncName {
	case "mathRound":
		p1 := m.resolveParam(fn.Params[0], activeLit)
		return FastValMathRound(p1)
	default:
		panic("encountered unexpected function name")
	}
}

func (m *Matcher) resolveParam(in interface{}, activeLit *FastVal) FastVal {
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

func (m *Matcher) matchOp(op *OpNode, litVal *FastVal) error {
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
	case OpTypeNotEquals:
		opRes = !lhsVal.Equals(rhsVal)
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

func (m *Matcher) matchElems(token tokenType, tokenData []byte, elems map[string]*ExecNode) error {
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
			token, _, err := m.tokens.Step()
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

		token, tokenData, err := m.tokens.Step()
		if err != nil {
			return err
		}
		if token == tknObjectEnd {
			return nil
		}

		// TODO(brett19): These byte-string conversion pieces are a bit wierd
		var keyBytes []byte
		if token == tknString {
			keyBytes = keyLitParse.ParseString(tokenData)
		} else if token == tknEscString {
			keyBytes = keyLitParse.ParseEscString(tokenData)
		} else {
			panic("expected literal")
		}

		token, _, err = m.tokens.Step()
		if err != nil {
			return err
		}
		if token != tknObjectKeyDelim {
			panic("expected object key delimiter")
		}

		token, tokenData, err = m.tokens.Step()
		if err != nil {
			return err
		}

		if keyElem, ok := elems[string(keyBytes)]; ok {
			// Run the execution node that applies to this particular
			// key of the object.
			m.matchExec(token, tokenData, keyElem)

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

func (m *Matcher) matchLoop(token tokenType, tokenData []byte, loop *LoopNode) error {
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
			token, _, err := m.tokens.Step()
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

		token, tokenData, err := m.tokens.Step()
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
		err = m.matchExec(token, tokenData, loop.Node)
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

func (m *Matcher) matchAfter(node *AfterNode) error {
	savePos := m.tokens.Position()

	// Run loop matching
	for _, loop := range node.Loops {
		if slot, ok := loop.Target.(SlotRef); ok {
			slotInfo := m.slots[slot.Slot-1]

			m.tokens.Seek(slotInfo.start)
			token, tokenData, err := m.tokens.Step()

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

func (m *Matcher) matchExec(token tokenType, tokenData []byte, node *ExecNode) error {
	startPos := m.tokens.Position()
	endPos := -1

	// The start position needs to include the token we already parsed, so lets
	// back up our position based on how long that is...
	// TODO(brett19): We should probably find a more optimal way to handle this...
	startPos -= len(tokenData)

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
		if len(node.Elems) == 0 {
			// If we have no element handlers, we can just skip the whole thing...
			m.skipValue(token)
		} else {
			err := m.matchElems(token, tokenData, node.Elems)
			if err != nil {
				return nil
			}

			if m.buckets.IsResolved(0) {
				return nil
			}
		}
	} else if token == tknArrayStart {
		if len(node.Loops) == 0 {
			// If we have no loop handlers, we can just skip the whole thing...
			m.skipValue(token)
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
	} else {
		panic("invalid token read")
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

func (m *Matcher) Match(data []byte) (bool, error) {
	m.tokens.Reset(data)

	token, tokenData, err := m.tokens.Step()
	if err != nil {
		return false, err
	}

	err = m.matchExec(token, tokenData, m.def.ParseNode)
	if err != nil {
		return false, err
	}

	// Resolve any outstanding buckets in the tree.  This is required for
	// operators such as NOT and NEOR to correctly be resolved.
	m.buckets.Resolve()

	return m.buckets.IsTrue(0), nil
}

func (m *Matcher) ExpressionMatched(expressionIdx int) bool {
	binTreeIdx := m.def.MatchBuckets[expressionIdx]
	return m.buckets.IsResolved(binTreeIdx) &&
		m.buckets.IsTrue(binTreeIdx)
}
