// +build perf

package gojsonsm

import (
	"testing"
)

func generateRandomData(mbsToGenerate int) ([][]byte, int, error) {
	avgBytesOfOneRecord := 1800
	rowsToGenerate := mbsToGenerate * 1000000 / avgBytesOfOneRecord
	data := make([][]byte, rowsToGenerate)
	totalBytes, err := genRandomUsers(32534059803498589, data)
	if err != nil {
		return nil, 0, err
	}
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
	m := NewFastMatcher(matchDef)

	b.SetBytes(int64(totalBytes))
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		for i := 0; i < len(data); i++ {
			_, err := m.Match(data[i])

			if err != nil {
				b.Fatalf("FastMatcher error: %s", err)
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
