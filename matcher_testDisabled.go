// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"encoding/json"
	"fmt"
	"github.com/Knetic/govaluate"
	"github.com/davecgh/go-spew/spew"
	"testing"
)

func generateRandomData(mbsToGenerate int) ([][]byte, int, error) {
	//	data := make([][]byte, 1)
	//	totalBytes, err := genRandomUsers(32534059803498589, data)
	//	if err != nil {
	//		return nil, 0, err
	//	}
	//	fmt.Printf("TotalBytes: %v TotalEntries: %v\n", totalBytes, len(data))
	avgBytesOfOneRecord := 1800
	rowsToGenerate := mbsToGenerate * 1000000 / avgBytesOfOneRecord
	data := make([][]byte, rowsToGenerate)
	totalBytes, err := genRandomUsers(32534059803498589, data)
	if err != nil {
		return nil, 0, err
	}
	fmt.Printf("MBs To Generate: %v TotalBytes: %v TotalEntries: %v\n", mbsToGenerate, totalBytes, len(data))
	return data, totalBytes, nil
}

func BenchmarkMatcher(b *testing.B) {
	data, totalBytes, err := generateRandomData(1)
	if err != nil || len(data) == 0 {
		b.Fatalf("Data generation error: %s", err)
	}

	// name["first"]="Brett" OR (age<50 AND isActive=True)
	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "Brett"]
	  ],
	  ["and",
	    ["lessthan",
	      ["field", "age"],
	      ["value", 50]
	    ],
	    ["equals",
	      ["field", "isActive"],
	      ["value", true]
	    ]
	  ]
    ]`)
	expr, err := ParseJsonExpression(matchJson)
	if err != nil {
		b.Errorf("Failed to parse expression: %s", err)
		return
	}

	var trans Transformer
	matchDef := trans.Transform([]Expression{expr})
	m := NewMatcher(matchDef)

	b.SetBytes(int64(totalBytes))
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		for i := 0; i < len(data); i++ {
			_, err := m.Match(data[i])

			if err != nil {
				b.Fatalf("Matcher error: %s", err)
			}
		}
	}
}

func BenchmarkSlowMatcher(b *testing.B) {
	data, totalBytes, err := generateRandomData(1)
	if err != nil || len(data) == 0 {
		b.Fatalf("Data generation error: %s", err)
	}

	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "Brett"]
	  ],
	  ["and",
	    ["lessthan",
	      ["field", "age"],
	      ["value", 50]
	    ],
	    ["equals",
	      ["field", "isActive"],
	      ["value", true]
	    ]
	  ]
    ]`)
	expr, err := ParseJsonExpression(matchJson)
	if err != nil {
		b.Errorf("Failed to parse expression: %s", err)
		return
	}

	m := NewSlowMatcher([]Expression{expr})

	b.SetBytes(int64(totalBytes))
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		for i := 0; i < len(data); i++ {
			_, err := m.Match(data[i])

			if err != nil {
				b.Fatalf("Slow matcher error: %s", err)
			}
		}
	}
}

func BenchmarkFlexibleMatcher(b *testing.B) {
	data, totalBytes, err := generateRandomData(1)
	if err != nil || len(data) == 0 {
		b.Fatalf("Data generation error: %s", err)
	}

	m := NewFlexibleMatcher()

	// Expression reformatted:
	expression, err := govaluate.NewEvaluableExpression("firstName == 'Neil' || (age < 50 && isActive == true)")
	if err != nil {
		b.Fatalf("NewEvaluableExpression Error: %s", err)
		return
	}

	// Pre-make parameters and re-use
	parameters := NewParameterArray(3)
	b.SetBytes(int64(totalBytes))
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		for i := 0; i < len(data); i++ {
			_, err := m.Match(data[i], expression, *parameters)

			if err != nil {
				b.Fatalf("Matcher error: %s", err)
			}
		}
	}
}

func TestMatcher(t *testing.T) {
	data, _, err := generateRandomData(1)
	if err != nil || len(data) == 0 {
		t.Fatalf("Data generation error: %s", err)
	}

	var parsedData interface{}
	json.Unmarshal(data[0], &parsedData)
	t.Logf("Document: %s", spew.Sdump(parsedData))

	matchJson := []byte(`
	["or",
	  ["equals",
	    ["field", "name", "first"],
	    ["value", "Brett"]
	  ],
	  ["and",
	    ["lessthan",
	      ["field", "age"],
	      ["value", 50]
	    ],
	    ["equals",
	      ["field", "isActive"],
	      ["value", true]
	    ]
	  ]
    ]`)

	// Disabling for now since FlexibleMatcher isn't flexible enough for anyin
	//	matchJson := []byte(`
	//	["or",
	//	  ["equals",
	//	    ["field", "name", "first"],
	//	    ["value", "Brett"]
	//	  ],
	//	  ["and",
	//	    ["lessthan",
	//	      ["field", "age"],
	//	      ["value", 50]
	//	    ],
	//	    ["equals",
	//	      ["field", "isActive"],
	//	      ["value", true]
	//	    ]
	//	  ],
	//	  ["anyin",
	//	     1,
	//	     ["field", "tags"],
	//	     ["equals",
	//	       ["field", 1],
	//	       ["value", "nothing"]
	//	     ]
	//	   ]
	//    ]`)
	expr, err := ParseJsonExpression(matchJson)
	if err != nil {
		t.Errorf("Failed to parse expression: %s", err)
		return
	}

	t.Logf("Expression:\n%s", expr)

	matchExpression, err := govaluate.NewEvaluableExpression("firstName == 'Brett' || (age < 50 && isActive == true)")
	if err != nil {
		t.Fatalf("NewEvaluableExpression Error: %s", err)
		return
	}
	parameters := NewParameterArray(3)

	var stats ExpressionStats
	stats.Scan(expr)

	t.Logf("Stats:\n%s", stats)

	var trans Transformer
	matchDef := trans.Transform([]Expression{expr})

	t.Logf("Transformed:\n%s", matchDef)

	mismatched := 0.0
	totalMatched := 0.0
	for i := 0; i < len(data); i++ {
		m := NewMatcher(matchDef)
		matched, err := m.Match(data[i])
		if err != nil {
			t.Errorf("Matcher error: %s", err)
			continue
		}

		em := NewFlexibleMatcher()
		flexMatched, err := em.Match(data[i], matchExpression, *parameters)
		if err != nil {
			t.Errorf("FlexMatcher error: %s", err)
			continue
		}

		totalMatched++
		if matched != flexMatched {
			var unmd map[string]interface{}
			err = json.Unmarshal(data[i], &unmd)
			if err != nil {
				t.Errorf("Unable to unmarshal: %s", err.Error())
			}
			t.Errorf("Matchers did not agree for item: %s", spew.Sdump(unmd))
			t.Errorf("  Fast Matcher: %t", matched)
			t.Errorf("  Flex Matcher: %t", flexMatched)
			mismatched++
			continue
		}
	}

	fmt.Printf("Total matches: %v - Mismatches: %v - pass rate: %v\n", totalMatched, mismatched, (totalMatched-mismatched)/totalMatched)
}

/*
project:
  x.*.*
  y.*.*
for a in .x
  for b in a
	for c in .y
	  for d in c
		d.dead OR b.value=d.value
	  end
	end
  end
end
*/
/*
expr := AndExpr{
	[]Expression{
		OrExpr{
			[]Expression{
				AnyInExpr{
					1,
					FieldExpr{"", []string{"x"}},
					AnyInExpr{
						2,
						FieldExpr{"$1", nil},
						AnyInExpr{
							3,
							FieldExpr{"", []string{"y"}},
							AnyInExpr{
								4,
								FieldExpr{"$3", nil},
								OrExpr{
									[]Expression{
										EqualsExpr{
											FieldExpr{"$4", []string{"dead"}},
											ValueExpr{true},
										},
										EqualsExpr{
											FieldExpr{"$2", []string{"value"}},
											FieldExpr{"$4", []string{"value"}},
										},
									},
								},
							},
						},
					},
				},
				EqualsExpr{
					FieldExpr{"", []string{"a"}},
					FieldExpr{"", []string{"b"}},
				},
				EqualsExpr{
					FieldExpr{"", []string{"z"}},
					ValueExpr{14},
				},
			},
		},
		EqualsExpr{
			FieldExpr{"", []string{"bob"}},
			ValueExpr{true},
		},
	},
}
*/
/*
expr := AndExpr{
	EqualsExpr{
		FieldExpr{0, []string{"userid"}},
		FieldExpr{0, []string{"id"}},
	},
	OrExpr{
		EqualsExpr{
			FieldExpr{0, []string{"state", "active"}},
			ValueExpr{true},
		},
		EqualsExpr{
			FieldExpr{0, []string{"state", "type"}},
			ValueExpr{"active"},
		},
	},
	AnyInExpr{
		1,
		FieldExpr{0, []string{"roles"}},
		OrExpr{
			AndExpr{
				EqualsExpr{
					FieldExpr{1, []string{"name"}},
					ValueExpr{"user"},
				},
				LessThanExpr{
					FieldExpr{0, []string{"userid"}},
					ValueExpr{1000},
				},
			},
			AndExpr{
				EqualsExpr{
					FieldExpr{1, []string{"name"}},
					ValueExpr{"admin"},
				},
				GreaterEqualExpr{
					FieldExpr{0, []string{"userid"}},
					ValueExpr{1000},
				},
			},
		},
	},
	AnyInExpr{
		2,
		FieldExpr{0, []string{"tags"}},
		OrExpr{
			EqualsExpr{
				FieldExpr{2, nil},
				ValueExpr{"couchbase"},
			},
			EqualsExpr{
				FieldExpr{2, []string{"name"}},
				ValueExpr{"couchbase"},
			},
		},
	},
}
//*/
/*
expr := OrExpr{
	AndExpr{
		EqualsExpr{
			FieldExpr{"", []string{"type"}},
			ValueExpr{"route"},
		},
		OrExpr{
			EqualsExpr{
				FieldExpr{"", []string{"sourceairport"}},
				ValueExpr{"MCO"},
			},
			EqualsExpr{
				FieldExpr{"", []string{"destinationairport"}},
				ValueExpr{"MCO"},
			},
		},
	},
	AndExpr{
		EqualsExpr{
			FieldExpr{"", []string{"type"}},
			ValueExpr{"airline"},
		},
		EqualsExpr{
			FieldExpr{"", []string{"icao"}},
			ValueExpr{"MCO"},
		},
	},
}
*/
/*
expr := OrExpr{
	[]Expression{
		EqualsExpr{
			FieldExpr{"", []string{"x"}},
			FieldExpr{"", []string{"y"}},
		},
		EqualsExpr{
			FieldExpr{"", []string{"type"}},
			ValueExpr{"airline"},
		},
	},
}
*/
/*matchJson := []byte(`
["and",
  ["equals",
	["field", "userid"],
	["field", "id"]
  ],
  ["or",
	["equals",
	  ["field", "state", "active"],
	  ["value", true]
	],
	["equals",
	  ["field", "state", "type"],
	  ["value", "active"]
	]
  ],
  ["anyin", 1,
	["field", "roles"],
	["or",
	  ["and",
		["equals",
		  ["field", 1, "name"],
		  ["value", "user"]
		],
		  ["lessthan",
			["field", "userid"],
			["value", 1000]
		  ]
	  ],
	  ["and",
		["equals",
		  ["field", 1, "name"],
		  ["value", "admin"]
		],
		["greaterequal",
		  ["field", "userid"],
		  ["value", 1000]
		]
	  ]
	]
  ],
  ["anyin", 2,
	["field", "tags"],
	["or",
	  ["equals",
		["field", 2],
		["value", "couchbase"]
	  ],
	  ["equals",
		["field", 2, "name"],
		["value", "couchbase"]
	  ]
	]
  ]
]`)*/
/*
matchJson := []byte(`
["and",
  ["lessthan",
	["field", "age"],
	["value", 50]
  ],
  ["anyin", 1,
	["field", "friends"],
	["or",
	  ["equals",
		["field", 1, "age"],
		["field", "age"]
	  ],
	  ["equals",
		["field", 1, "age"],
		["value", 50]
	  ]
	]
  ]
]`)
*/
