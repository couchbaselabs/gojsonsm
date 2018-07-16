// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"encoding/json"
	"io/ioutil"
)

var cachedTestData [][]byte

func getTestPeopleDocs() [][]byte {
	if cachedTestData == nil {
		bytes, err := ioutil.ReadFile("testdata/people.json")
		if err != nil {
			panic(err)
		}

		var docsArr []json.RawMessage
		err = json.Unmarshal(bytes, &docsArr)
		if err != nil {
			panic(err)
		}

		cachedTestData = make([][]byte, len(docsArr))
		for i, docBytes := range docsArr {
			cachedTestData[i] = docBytes
		}
	}

	return cachedTestData
}
