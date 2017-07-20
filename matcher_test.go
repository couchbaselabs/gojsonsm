package gojsonsm

import (
	"testing"
	"encoding/json"
	"github.com/davecgh/go-spew/spew"
)

func BenchmarkMatcher(b *testing.B) {
	data := make([][]byte, 1)
	totalBytes, err := genRandomUsers(32534059803498589, data)
	if err != nil {
		b.Fatalf("Failed to generate test data: %s", err)
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

	var trans Transformer
	matchDef := trans.Transform(expr)
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
	data := make([][]byte, 1)
	totalBytes, err := genRandomUsers(32534059803498589, data)
	if err != nil {
		b.Fatalf("Failed to generate test data: %s", err)
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

	m := NewSlowMatcher(expr)

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

func TestMatcher(t *testing.T) {
	data := make([][]byte, 1)
	_, err := genRandomUsers(32534059803498589, data)
	if err != nil {
		t.Fatalf("Failed to generate test data: %s", err)
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
	  ],
	  ["anyin",
	     1,
	     ["field", "tags"],
	     ["equals",
	       ["field", 1],
	       ["value", "nothing"]
	     ]
	   ]
    ]`)
	expr, err := ParseJsonExpression(matchJson)
	if err != nil {
		t.Errorf("Failed to parse expression: %s", err)
		return
	}

	t.Logf("Expression:\n%s", expr)

	var stats ExpressionStats
	stats.Scan(expr)

	t.Logf("Stats:\n%s", stats)

	var trans Transformer
	matchDef := trans.Transform(expr)

	t.Logf("Transformed:\n%s", matchDef)

	for i := 0; i < len(data); i++ {
		m := NewMatcher(matchDef)
		sm := NewSlowMatcher(expr)

		matched, err := m.Match(data[i])
		if err != nil {
			t.Errorf("Matcher error: %s", err)
			continue
		}

		slowMatched, err := sm.Match(data[i])
		if err != nil {
			t.Errorf("Slow matcher error: %s", err)
			continue
		}

		if matched != slowMatched {
			t.Errorf("Matchers did not agree for item %d", i)
			t.Errorf("  Fast Matcher: %t", matched)
			t.Errorf("  Slow Matcher: %t", slowMatched)
			continue
		}
	}
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
