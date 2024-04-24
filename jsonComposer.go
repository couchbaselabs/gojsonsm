// Copyright 2018-2019 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
)

type jsonObjComposer struct {
	// should have enough length
	body []byte
	// cursor
	pos int
	// if the newly composed json has atleast one field
	atLeastOneFieldLeft bool
}

func (composer *jsonObjComposer) Write(data []byte) {
	copy(composer.body[composer.pos:composer.pos+len(data)], data)
	composer.pos += len(data)
}

func (composer *jsonObjComposer) Commit(stepBackAndClose bool) (int, bool) {
	// close the JSON object after stepping back by one position explicity if requested
	if stepBackAndClose && composer.pos-1 >= 0 {
		if composer.body[composer.pos-1] != '{' {
			composer.pos--
		}
		composer.body[composer.pos] = '}'
		composer.pos++
	}

	// if the body is anything other than {}, we have atleast one field inside it
	composer.atLeastOneFieldLeft = composer.pos > 2

	return composer.pos, composer.atLeastOneFieldLeft
}

// Given a byte encoded json object - "src", a list of keys of items to remove from src - "remove",
// the function removes the items from "src" and places them in "removed" and the remaining json object is stored in "dst"
// returns (final length of dst, number of items removed, if there are any items left in dst at the end, error)
// Caller has the ability to allocate memory for "dst" and "removed". If nil is passed, only then memory is allocated.
func MatchAndRemoveItemsFromJsonObject(src []byte, remove []string, dst []byte, removed map[string][]byte) (int, int, bool, error) {
	var removedLen int
	if removed == nil {
		removed = make(map[string][]byte)
	}

	if dst == nil {
		dst = make([]byte, len(src))
	}

	composer := &jsonObjComposer{
		body:                dst,
		pos:                 0,
		atLeastOneFieldLeft: false,
	}

	tokenizer := &jsonTokenizer{}
	tokenizer.Reset(src)

	var tknType tokenType
	var tknLen, dstLen int
	var tkn []byte
	var err error
	var atleastOneFieldLeft, removedLastItem bool
	var depth int
	for tknType != tknEnd {
		tknType, tkn, tknLen, err = tokenizer.Step()
		if err != nil {
			return 0, 0, false, fmt.Errorf("error stepping to next token, src=%s, pos=%v, err=%v", src, tokenizer.Position(), err)
		}

		switch tknType {
		case tknString:
			// depth 1 strings are JSON object keys, need to check if it is the key to remove
			if depth != 1 {
				composer.Write(tkn)
				continue
			}
		case tknObjectStart:
			fallthrough
		case tknArrayStart:
			depth++
			composer.Write(tkn)
			continue
		case tknObjectEnd:
			fallthrough
		case tknArrayEnd:
			depth--
			if depth < 0 {
				return 0, 0, false, fmt.Errorf("invalid JSON object")
			}
			composer.Write(tkn)
			continue
		case tknEnd:
			continue
		default:
			composer.Write(tkn)
			continue
		}

		// strip off the quotes from the string
		key := tkn[1 : tknLen-1]

		matched := false
		for _, keyToRemove := range remove {
			if BytesEqualsString(key, keyToRemove) {
				matched = true

				// ":"
				tknType, _, _, err = tokenizer.Step()
				if err != nil || tknType != tknObjectKeyDelim {
					return 0, 0, false, fmt.Errorf("error stepping to next token, expecting :, src=%s, pos=%v, err=%v", src, tokenizer.Position(), err)
				}

				// parse the corresponding value
				valStart := tokenizer.Position()
				valEnd := valStart
				valueDepth := 0
				done := false

				for !done {
					tknType, _, _, err = tokenizer.Step()
					if err != nil {
						return 0, 0, false, fmt.Errorf("error stepping to next token, expecting JSON value, src=%s, pos=%v, err=%v", src, tokenizer.Position(), err)
					}

					valEnd = tokenizer.Position()

					switch tknType {
					case tknObjectStart:
						fallthrough
					case tknArrayStart:
						valueDepth++
					case tknObjectEnd:
						fallthrough
					case tknArrayEnd:
						valueDepth--
						if valueDepth == 0 {
							done = true
						} else if valueDepth < 0 {
							return 0, 0, false, fmt.Errorf("invalid JSON object, src=%s, pos=%v", src, tokenizer.Position())
						}
					case tknString:
						fallthrough
					case tknEscString:
						fallthrough
					case tknInteger:
						fallthrough
					case tknNumber:
						fallthrough
					case tknNull:
						fallthrough
					case tknTrue:
						fallthrough
					case tknFalse:
						if valueDepth == 0 {
							done = true
						}
					case tknEnd:
						return 0, 0, false, fmt.Errorf("unexpected EOF, src=%s, pos=%v", src, tokenizer.Position())
					default:
						// tknListDelim, tknObjectKeyDelim, tknUnknown
					}
				}
				removed[keyToRemove] = src[valStart:valEnd]
				removedLen++

				// can be tknObjectEnd or tknListDelim
				// if it is tknObjectEnd, we have to step back, remove a tknListDelim and place a tknObjectEnd before commiting
				// if it is tknListDelim, don't write it
				tknType, tkn, _, err = tokenizer.Step()
				if err != nil || (tknType != tknObjectEnd && tknType != tknListDelim) {
					return 0, 0, false, fmt.Errorf("error stepping to next token, expecting separator or objectEnd, got=%s, src=%s, pos=%v", tkn, src, tokenizer.Position())
				}

				if tknType == tknObjectEnd {
					// this was the last item
					if depth == 1 {
						removedLastItem = true
					}
					depth--
					if depth < 0 {
						return 0, 0, false, fmt.Errorf("invalid JSON object")
					}
				}
			}
		}

		if !matched {
			// okay to write this item
			composer.Write(tkn)
		}
	}

	if depth != 0 {
		return 0, 0, false, fmt.Errorf("invalid input, needs to be a JSON object")
	}

	dstLen, atleastOneFieldLeft = composer.Commit(removedLastItem)

	return dstLen, removedLen, atleastOneFieldLeft, nil
}

// check whether source byte array contains the same string as target string
// this impl avoids converting byte array to string
func BytesEqualsString(source []byte, target string) bool {
	if len(source) != len(target) {
		return false
	}
	for i := 0; i < len(target); i++ {
		if target[i] != source[i] {
			return false
		}
	}
	return true
}
