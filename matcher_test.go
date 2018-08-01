// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

func runExprMatchTest(t *testing.T, expr Expression, expectedDocIDs []string) {
	parseDocID := func(docBytes []byte) string {
		var data struct {
			ID string `json:"_id"`
		}
		json.Unmarshal(docBytes, &data)
		return data.ID
	}

	docs := getTestPeopleDocs()

	var trans Transformer
	matchDef := trans.Transform([]Expression{expr})

	var matchedDocIDs []string

	for _, doc := range docs {
		m := NewMatcher(matchDef)
		matched, err := m.Match(doc)
		if err != nil {
			t.Errorf("Matcher error: %s", err)
			continue
		}

		if matched {
			docID := parseDocID(doc)
			matchedDocIDs = append(matchedDocIDs, docID)
		}
	}

	// Presort for easier matching
	sort.Strings(matchedDocIDs)
	sort.Strings(expectedDocIDs)

	documentsMatched := true

	if documentsMatched {
		if len(matchedDocIDs) != len(expectedDocIDs) {
			documentsMatched = false
		}
	}

	if documentsMatched {
		for i, expectedDocID := range expectedDocIDs {
			if matchedDocIDs[i] != expectedDocID {
				documentsMatched = false
				break
			}
		}
	}

	if !documentsMatched {
		t.Errorf("Matched documents did not match expectations:")
		t.Errorf("  Expected: %s", strings.Join(expectedDocIDs, ", "))
		t.Errorf("  Matched: %s", strings.Join(matchedDocIDs, ", "))
		t.Errorf("  Match Tree:")
		t.Errorf("  Matcher Definition:\n%s", matchDef.String())
	}
}

func runJSONExprMatchTest(t *testing.T, data string, expectedDocIDs []string) {
	expr, err := ParseJsonExpression([]byte(data))
	if err != nil {
		panic(err)
	}

	runExprMatchTest(t, expr, expectedDocIDs)
}

func TestMatcherStringEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
		["equals",
	    ["field", "name"],
	    ["value", "Daphne Sutton"]
	  ]
	`, []string{
		"5b47eb0936ff92a567a0307e",
	})
}

func TestMatcherNumericEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
		["equals",
	    ["field", "age"],
	    ["value", 25]
	  ]
	`, []string{
		"5b47eb091f57571d3c3b1aa1",
	})
}

func TestMatcherFloatEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
		["equals",
	    ["field", "latitude"],
	    ["value", -40.262556]
	  ]
	`, []string{
		"5b47eb096b1d911c0b9492fb",
	})
}

func TestMatcherTrueEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
		["equals",
	    ["field", "isActive"],
	    ["value", true]
	  ]
	`, []string{
		"5b47eb0936ff92a567a0307e",
		"5b47eb0950e9076fc0aecd52",
		"5b47eb095c3ad73b9925f7f8",
		"5b47eb0962222a37d066e231",
		"5b47eb09996a4154c35b2f98",
		"5b47eb098eee4b4c4330ec64",
	})
}

func TestMatcherFalseEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
		["equals",
	    ["field", "isActive"],
	    ["value", false]
	  ]
	`, []string{
		"5b47eb096b1d911c0b9492fb",
		"5b47eb093771f06ced629663",
		"5b47eb09ffac5a6ce37042e7",
		"5b47eb091f57571d3c3b1aa1",
	})
}

func TestMatcherNotTrueEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
	  ["not",
	    ["equals",
		  ["field", "isActive"],
		  ["value", true]
	    ]
	  ]
	`, []string{
		"5b47eb096b1d911c0b9492fb",
		"5b47eb093771f06ced629663",
		"5b47eb09ffac5a6ce37042e7",
		"5b47eb091f57571d3c3b1aa1",
	})
}

func TestMatcherMissingNotEquals(t *testing.T) {
	// This tests a specific case where a missing value should actually
	// result in a truthy result in the expression.  Due to the nature
	// of our bintree implementation, this needs special handling.
	runJSONExprMatchTest(t, `
		["not",
		  ["equals",
	      ["field", "sometimesValue"],
		  ["value", "true"]
		]
	  ]
	`, []string{
		"5b47eb0950e9076fc0aecd52",
		"5b47eb093771f06ced629663",
		"5b47eb09ffac5a6ce37042e7",
		"5b47eb095c3ad73b9925f7f8",
		"5b47eb0962222a37d066e231",
		"5b47eb09996a4154c35b2f98",
		"5b47eb091f57571d3c3b1aa1",
		"5b47eb098eee4b4c4330ec64",
	})
}

func TestMatcherExists(t *testing.T) {
	// This tests a specific case where a missing value should actually
	// result in a truthy result in the expression.  Due to the nature
	// of our bintree implementation, this needs special handling.
	runJSONExprMatchTest(t, `
	  ["exists",
	    ["field", "sometimesValue"]
	  ]
	`, []string{
		"5b47eb0936ff92a567a0307e",
		"5b47eb096b1d911c0b9492fb",
		"5b47eb0950e9076fc0aecd52",
	})
}

func TestMatcherNotExists(t *testing.T) {
	// This tests a specific case where a missing value should actually
	// result in a truthy result in the expression.  Due to the nature
	// of our bintree implementation, this needs special handling.
	runJSONExprMatchTest(t, `
	  ["notexists",
	    ["field", "sometimesValue"]
	  ]
	`, []string{
		"5b47eb093771f06ced629663",
		"5b47eb09ffac5a6ce37042e7",
		"5b47eb095c3ad73b9925f7f8",
		"5b47eb0962222a37d066e231",
		"5b47eb09996a4154c35b2f98",
		"5b47eb091f57571d3c3b1aa1",
		"5b47eb098eee4b4c4330ec64",
	})
}

func TestMatcherDisparateTypeEquals(t *testing.T) {
	// TODO(brett19): Should probably discuss whether type-cast equals
	// actually makes sense... This validates that these something like:
	//  (true == "thisShouldBeABoolean") === true
	// which may not actually make a whole lot of sense...
	runJSONExprMatchTest(t, `
		["equals",
	    ["field", "sometimesValue"],
	    ["value", "thisShouldBeABoolean"]
	  ]
	`, []string{
		"5b47eb0936ff92a567a0307e",
		"5b47eb096b1d911c0b9492fb",
	})
}

func TestMatcherSometimesMissingBoolEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
		["equals",
	    ["field", "sometimesValue"],
	    ["value", false]
	  ]
	`, []string{
		"5b47eb0936ff92a567a0307e",
		"5b47eb096b1d911c0b9492fb",
	})
}

func TestMatcherMissingStringEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
		["equals",
	    ["field", "someValueWhichNeverExists"],
	    ["value", "hello"]
	  ]
	`, []string{})
}

func TestMatcherAnyInEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
		["anyin",
			1,
			["field", "tags"],
			["equals",
				["field", 1],
				["value", "cillum"]
			]
		]
	`, []string{
		"5b47eb0936ff92a567a0307e",
		"5b47eb09ffac5a6ce37042e7",
		"5b47eb095c3ad73b9925f7f8",
	})
}

func TestMatcherNestedAnyInAnyInEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
		["anyin",
			1,
			["field", "nestedArray"],
			["anyin",
				2,
				["field", 1],
				["equals",
					["field", 2],
					["value", "g"]
				]
			]
		]
	`, []string{
		"5b47eb0936ff92a567a0307e",
	})
}

func TestMatcherNestedAnyInAnyInEqualsNoMatch(t *testing.T) {
	runJSONExprMatchTest(t, `
		["anyin",
			1,
			["field", "nestedArray"],
			["anyin",
				2,
				["field", 1],
				["equals",
					["field", 2],
					["value", "z"]
				]
			]
		]
	`, []string{})
}

func TestMatcherEveryInEquals(t *testing.T) {
	// TODO(brett19): Should probably discuss whether this logic really makes
	// sense.  In its current form, an empty array will always match every-in,
	// but would fail for an any-every-in. Hmm...
	runJSONExprMatchTest(t, `
		["everyin",
			1,
			["field", "testArray"],
			["equals",
				["field", 1],
				["value", "jewels"]
			]
		]
	`, []string{
		"5b47eb0936ff92a567a0307e",
		"5b47eb09ffac5a6ce37042e7",
	})
}

func TestMatcherAnyEveryInEquals(t *testing.T) {
	runJSONExprMatchTest(t, `
		["anyeveryin",
			1,
			["field", "testArray"],
			["equals",
				["field", 1],
				["value", "jewels"]
			]
		]
	`, []string{
		"5b47eb0936ff92a567a0307e",
	})
}
