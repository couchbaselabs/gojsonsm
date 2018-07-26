// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
)

type varData struct {
	start int
	size  int
}

type Matcher struct {
	def       MatchDef
	variables []varData
	buckets   *binTreeState
	tokens    jsonTokenizer
}

func NewMatcher(def *MatchDef) *Matcher {
	return &Matcher{
		def:       *def,
		variables: make([]varData, def.NumFetches),
		buckets:   def.MatchTree.NewState(),
	}
}

func (m *Matcher) Reset() {
	m.variables = m.variables[:0]
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

func (m *Matcher) resolveParam(in interface{}) FastVal {
	if opVal, ok := in.(VarRef); ok {
		panic(fmt.Sprintf("Cannot read %d", opVal.VarIdx))
	}
	if opValV, ok := in.(FastVal); ok {
		return opValV
	} else {
		panic(fmt.Sprintf("unexpected op value: %#v", in))
	}
}

func (m *Matcher) matchExec(token tokenType, tokenData []byte, node *ExecNode) error {
	startPos := m.tokens.pos
	endPos := -1

	if isLiteralToken(token) {
		var litParse fastLitParser

		// TODO(brett19): Move the litVal generation to be lazy-evaluated by the
		// op execution below so we avoid performing any translations when the op
		// is already resolved by something else.

		// Parse the literal token from the tokenizer into a FastVal value
		// to be used for op execution below.
		litVal := litParse.Parse(token, tokenData)

		for _, op := range node.Ops {
			if m.buckets.IsResolved(int(op.BucketIdx)) {
				// If the bucket for this op is already resolved  in the binary tree,
				// we don't need to perform the op and can just skip it.
				continue
			} else {
				var opVal FastVal
				if op.Rhs != nil {
					opVal = m.resolveParam(op.Rhs)
				}

				var opRes bool
				switch op.Op {
				case OpTypeEquals:
					opRes = litVal.Equals(opVal)
				case OpTypeNotEquals:
					opRes = !litVal.Equals(opVal)
				case OpTypeLessThan:
					opRes = litVal.Compare(opVal) < 0
				case OpTypeLessEquals:
					opRes = litVal.Compare(opVal) <= 0
				case OpTypeGreaterThan:
					opRes = litVal.Compare(opVal) > 0
				case OpTypeGreaterEquals:
					opRes = litVal.Compare(opVal) >= 0
				case OpTypeMatches:
					opRes = litVal.Matches(opVal)
				default:
					panic("invalid op type")
				}

				// Mark the result of this operation
				m.buckets.MarkNode(int(op.BucketIdx), opRes)

				// Check if running this values ops has resolved the entirety
				// of the expression, if so we can leave immediately.
				if m.buckets.IsResolved(0) {
					return nil
				}
			}
		}

		return nil
	} else if token == tknObjectStart {
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

			if keyElem, ok := node.Elems[string(keyBytes)]; ok {
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
				if loopIdx != 0 {
					// If this is not the first loop, we will need to reset back to the
					// begining of the array the loops are scanning.  In the future, perhaps
					// we can add support for parallel ExecNode handling and do it in one pass.
					m.tokens.Seek(savePos)
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

				loopBucketIdx := int(loop.BucketIdx)

				// We need to mark the stall index on our binary tree so that
				// resolution of a loop iteration does not propagate up the tree
				// and cause resolution of the entire expression.
				previousStallIndex := m.buckets.SetStallIndex(loopBucketIdx)

				// Scan through all the values in the loop
				for i := 0; ; i++ {
					// If this is not the first entry in the array, there should be a
					// list delimiter ('c') that shows up in the input first.
					if i != 0 {
						token, _, err := m.tokens.Step()
						if err != nil {
							return err
						}

						if token == tknArrayEnd {
							break
						}
						if token != tknListDelim {
							panic("expected array element delimiter")
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
					m.matchExec(token, tokenData, loop.Node)

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

	endPos = m.tokens.pos
	if node.StoreId > 0 {
		varData := &m.variables[node.StoreId-1]
		varData.start = startPos
		varData.size = endPos - startPos
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

	// If the DAG was not resolved, it means that conditions
	// were not encountered (the document was missing parts of
	// the expected paths).
	if !m.buckets.IsResolved(0) {
		return false, nil
	}

	return m.buckets.IsTrue(0), nil
}

func (m *Matcher) ExpressionMatched(expressionIdx int) bool {
	binTreeIdx := m.def.MatchBuckets[expressionIdx]
	return m.buckets.IsResolved(binTreeIdx) &&
		m.buckets.IsTrue(binTreeIdx)
}
