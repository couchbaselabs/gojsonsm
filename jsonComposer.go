// Copyright 2024-Present Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
)

var (
	ErrNilComposer        error = fmt.Errorf("nil composer")
	ErrNilData            error = fmt.Errorf("nil data")
	ErrInvalidJSON        error = fmt.Errorf("invalid JSON object")
	ErrUnexpectedEOF      error = fmt.Errorf("unexpected EOF")
	ErrInsufficientMemory error = fmt.Errorf("insufficient memory allocated for dst, cannot proceed")
	ErrUnrecognisedToken  error = fmt.Errorf("unrecognised token in the JSON object")
)

type jsonObjComposer struct {
	// should have enough length
	body []byte
	// cursor
	pos int
	// if the newly composed json has atleast one field
	atLeastOneFieldLeft bool
	// the last written token type
	prevTokenType tokenType
}

// writes a given slice of data to the composer
// returns any errors in the process
func (composer *jsonObjComposer) Write(data []byte, tknType tokenType) error {
	if composer == nil {
		return ErrNilComposer
	}
	if data == nil {
		return ErrNilData
	}

	// if we are about to write objectEnd token i.e. "}", we should ensure that the previous position was not a ","
	// if it is ",", step back by one position and write a "}"
	if tknType == tknObjectEnd && composer.pos-1 >= 0 && composer.prevTokenType == tknListDelim {
		composer.pos--
	}
	n := copy(composer.body[composer.pos:composer.pos+len(data)], data)
	if n != len(data) {
		return ErrInsufficientMemory
	}
	composer.pos += len(data)
	composer.prevTokenType = tknType
	return nil
}

// finalizes the composer data and sets atLeastOneFieldLeft
// returns (length of composer data, if atleast one field is left in the newly composed JSON object)
func (composer *jsonObjComposer) Commit() (int, bool, error) {
	if composer == nil {
		return 0, false, ErrNilComposer
	}
	// if the body is anything other than {}, we have atleast one field inside it
	composer.atLeastOneFieldLeft = composer.pos > 2

	return composer.pos, composer.atLeastOneFieldLeft, nil
}

func handleError(err error) (int, int, bool, error) {
	return 0, 0, false, err
}

// Given a byte encoded json object - "src", a list of keys of items to remove from src - "remove",
// the function removes the items from "src" and places them in "removed" and the remaining json object is stored in "dst".
// It returns (final length of dst, number of items removed, if there are any items left in dst at the end, error).
// Caller has the ability to pass in pre-allocated byte slices for dst and pre-allocated map for removed. If nil is passed, only then memory is allocated.
func MatchAndRemoveItemsFromJsonObject(src []byte, remove []string, dst []byte, removed map[string][]byte) (int, int, bool, error) {
	if len(src) < 2 || src[0] != '{' || src[len(src)-1] != '}' {
		return handleError(ErrInvalidJSON)
	}
	if removed == nil {
		removed = make(map[string][]byte)
	}
	if dst == nil {
		dst = make([]byte, len(src))
	}

	composer := &jsonObjComposer{
		body: dst,
	}

	tokenizer := &jsonTokenizer{}
	tokenizer.Reset(src)

	var atleastOneFieldLeft bool
	var tknType, tknType1, tknType2, tknType3, tknType4 tokenType
	var potentialKey, potentialObjDelimiter, tkn []byte
	var depth, tknLen, dstLen, removedLen int
	var err error

	for tknType != tknEnd {
		tknType1, potentialKey, tknLen, err = tokenizer.Step()
		if err != nil {
			err = fmt.Errorf("error stepping to next token, src=%s, pos=%v, err=%v", src, tokenizer.Position(), err)
			return handleError(err)
		}
		tknType = tknType1

		switch tknType1 {
		case tknString:
			// string token can be a JSON key or a string JSON value
			// if the next token is ":", then potentialKey is a JSON key
			tknType2, potentialObjDelimiter, _, err = tokenizer.Step()
			if err != nil {
				err = fmt.Errorf("error stepping to next token, expecting :, src=%s, pos=%v, err=%v", src, tokenizer.Position(), err)
				return handleError(err)
			}
			tknType = tknType2

			if tknType2 != tknObjectKeyDelim {
				if tknType2 == tknUnknown {
					return handleError(ErrUnrecognisedToken)
				} else if tknType2 == tknEnd {
					return handleError(ErrUnexpectedEOF)
				}

				// potentialKey is not a JSON key, so don't try to match it with keys in "remove"
				err = composer.Write(potentialKey, tknType1)
				if err != nil {
					return handleError(err)
				}
				err = composer.Write(potentialObjDelimiter, tknType2)
				if err != nil {
					return handleError(err)
				}

				if tknType2 == tknObjectEnd {
					depth--
					if depth < 0 {
						return handleError(ErrInvalidJSON)
					}
				}
				continue
			}

			// potentialKey is indeed a JSON key, will try to match with "remove" next
		case tknObjectStart:
			fallthrough
		case tknArrayStart:
			depth++
			err = composer.Write(potentialKey, tknType1)
			if err != nil {
				return handleError(err)
			}
			continue
		case tknObjectEnd:
			fallthrough
		case tknArrayEnd:
			depth--
			if depth < 0 {
				return 0, 0, false, ErrInvalidJSON
			}
			err = composer.Write(potentialKey, tknType1)
			if err != nil {
				return handleError(err)
			}
			continue
		case tknEnd:
			continue
		case tknUnknown:
			return handleError(ErrUnrecognisedToken)
		default:
			// can be tknObjectKeyDelim, tknListDelim, tknEscString, tknInteger, tknNumber,
			// tknNull, tknTrue, tknFalse
			err = composer.Write(potentialKey, tknType1)
			if err != nil {
				return handleError(err)
			}
			continue
		}

		// Process to check if the JSON key parsed matches with the keys to remove

		// strip off the quotes from the string for matching
		key := potentialKey[1 : tknLen-1]

		matched := false
		for _, keyToRemove := range remove {
			if !BytesEqualsString(key, keyToRemove) {
				continue
			}

			matched = true

			// parse the corresponding value
			valStart := tokenizer.Position()
			valEnd := valStart
			valueDepth := 0
			valueFound := false

			for !valueFound {
				tknType3, _, _, err = tokenizer.Step()
				if err != nil {
					err = fmt.Errorf("error stepping to next token, expecting JSON value, src=%s, pos=%v, err=%v", src, tokenizer.Position(), err)
					return handleError(err)
				}
				tknType = tknType3

				valEnd = tokenizer.Position()

				switch tknType3 {
				case tknObjectStart:
					fallthrough
				case tknArrayStart:
					valueDepth++
				case tknObjectEnd:
					fallthrough
				case tknArrayEnd:
					valueDepth--
					if valueDepth == 0 {
						valueFound = true
					} else if valueDepth < 0 {
						return handleError(ErrInvalidJSON)
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
						valueFound = true
					}
				case tknEnd:
					return handleError(ErrUnexpectedEOF)
				case tknUnknown:
					return handleError(ErrUnrecognisedToken)
				default:
					// can be tknListDelim, tknObjectKeyDelim
				}
			}
			removed[keyToRemove] = src[valStart:valEnd]
			removedLen++

			// can be tknObjectEnd or tknListDelim
			// if it is tknListDelim, don't write it
			tknType4, tkn, _, err = tokenizer.Step()
			if err != nil || (tknType4 != tknObjectEnd && tknType4 != tknListDelim) {
				err = fmt.Errorf("error stepping to next token, expecting separator or objectEnd, got=%s, src=%s, pos=%v, err=%v", tkn, src, tokenizer.Position(), err)
				return handleError(err)
			}
			tknType = tknType4

			if tknType4 == tknObjectEnd {
				err = composer.Write(tkn, tknType)
				if err != nil {
					return handleError(err)
				}
				depth--
				if depth < 0 {
					return handleError(ErrInvalidJSON)
				}
			} else if tknType4 == tknUnknown {
				return handleError(ErrUnrecognisedToken)
			}
		}

		if !matched {
			// okay to write this item, since it didn't match
			err = composer.Write(potentialKey, tknType1)
			if err != nil {
				return handleError(err)
			}
			err = composer.Write(potentialObjDelimiter, tknType1)
			if err != nil {
				return handleError(err)
			}
		}
	}

	if depth != 0 {
		return handleError(ErrInvalidJSON)
	}

	dstLen, atleastOneFieldLeft, err = composer.Commit()
	if err != nil {
		return handleError(err)
	}

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
