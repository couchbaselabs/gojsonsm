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

	data := m.tokens.data
	pos := m.tokens.pos
	scanner := &m.tokens.scanner
	for {
		state := scanner.step(scanner, data[pos])
		pos++

		switch state {
		case scanBeginObject:
			depth++
		case scanEndObject:
			if depth == 0 {
				m.tokens.pos = pos
				return nil
			}
			depth--
		case scanBeginArray:
			depth++
		case scanEndArray:
			if depth == 0 {
				m.tokens.pos = pos
				return nil
			}
			depth--
		case scanError:
			return scanner.err
		case scanEnd:
			panic("unexpected EOF")
		}
	}
}

func (m *Matcher) skipValue(token TokenType) error {
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

func (m *Matcher) matchExec(token TokenType, tokenData []byte, node *ExecNode) error {
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
				if op.Op == OpTypeEquals {
					opRes = litVal.Equals(opVal)
				} else if op.Op == OpTypeLessThan {
					opRes = litVal.Compare(opVal) < 0
				} else if op.Op == OpTypeGreaterEquals {
					opRes = litVal.Compare(opVal) >= 0
				} else {
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

		for {
			token, tokenData, err := m.tokens.step()
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

			token, tokenData, err = m.tokens.step()
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
		for i := 0; ; i++ {
			token, _, err := m.tokens.step()
			if err != nil {
				return err
			}

			if token == tknArrayEnd {
				return nil
			}

			// TODO(brett19): We need to handle looping over arrays here.  Right
			// now we just ignore the array in its entirety, this is incorrect.
			m.skipValue(token)
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

	m.tokens.start(data)

	token, tokenData, err := m.tokens.step()
	if err != nil {
		return false, err
	}

	//	fmt.Printf("Parse node: %v\n", m.def.ParseNode)
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
