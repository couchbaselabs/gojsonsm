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
		//		fmt.Printf("NEIL DEBUG isLiteral token type: %v tokenData: %s\n", token, string(tokenData[:]))
		var litParse fastLitParser
		litVal := litParse.Parse(token, tokenData)

		//		fmt.Printf("LITERAL: `%v` %d %v\n", litVal, token, tokenData)
		for _, op := range node.Ops {
			//log.Printf("CHECK: %s %v", op.Op, op.Params)

			/*
				if m.buckets.IsResolved(0) && !m.buckets.IsTrue(0) {
					return nil
				}
			*/

			if m.buckets.IsResolved(int(op.BucketIdx)) {
				//				fmt.Printf("NEIL DEBUG bucketId: %v already resolved\n", op.BucketIdx)
				//				log.Printf("SKIPPED CHECK")
			} else {
				var opVal FastVal
				if op.Rhs != nil {
					opVal = m.resolveParam(op.Rhs)
				}

				if op.Op == OpTypeEquals {
					opRes := litVal.Equals(opVal)
					//					fmt.Printf("NEIL DEBUG fix equal type: %v RHS type %v\n", litVal.Type(), opVal.Type())
					//					fmt.Printf("NEIL DEBUG Marking bucket %v op: %v rhs: %v OpRes: %v\n",
					//						op.BucketIdx, opVal, op.Rhs, opRes)
					m.buckets.MarkNode(int(op.BucketIdx), opRes)
				} else if op.Op == OpTypeLessThan {
					opRes := litVal.Compare(opVal) < 0
					//					fmt.Printf("NEIL DEBUG Marking bucket %v op: %v rhs: %v OpRes: %v\n",
					//						op.BucketIdx, opVal, op.Rhs, opRes)
					m.buckets.MarkNode(int(op.BucketIdx), opRes)
				}
			}
		}

		return nil
	}

	if token == tknObjectStart {
		//		fmt.Printf("NEIL DEBUG tknObjectStart token type: %v tokenData: %s\n", token, tokenData)
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
				//log.Printf("MATCH ELEM: %s", keyBytes)
				m.matchExec(token, tokenData, keyElem)
			} else {
				//log.Printf("SKIP ELEM: %s", keyBytes)
				m.skipValue(token)
			}

			if m.buckets.IsResolved(0) {
				return nil
			}
		}
	}

	if token == tknArrayStart {
		for i := 0; ; i++ {
			token, _, err := m.tokens.step()
			if err != nil {
				return err
			}

			if token == tknArrayEnd {
				return nil
			}

			//log.Printf("ARRAY ELEM: %s", tokenData)

			m.skipValue(token)

			/*
				if m.buckets.IsResolved(0) && !m.buckets.IsTrue(0) {
					return nil
				}
			*/
		}
	}

	endPos = m.tokens.pos
	if node.StoreId > 0 {
		varData := &m.variables[node.StoreId-1]
		varData.start = startPos
		varData.size = endPos - startPos
	}

	panic("Invalid token read")
}

func (m *Matcher) Match(data []byte) (bool, error) {

	m.tokens.start(data)

	token, tokenData, err := m.tokens.step()
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
