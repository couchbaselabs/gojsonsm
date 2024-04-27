// Copyright 2024-Present Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_RemoveString(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name                       string
		src, expectedDst           []byte
		remove                     []string
		expectedRemoved            map[string][]byte
		expectedAtleastOnFieldLeft bool
		fails                      bool
	}{
		{
			name:                       "only one string item",
			src:                        []byte(`{"foo":"bar"}`),
			remove:                     []string{"foo"},
			expectedDst:                []byte(`{}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`"bar"`)},
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "two items, string item at the end",
			src:                        []byte(`{"foo1":"bar1","foo":"bar"}`),
			remove:                     []string{"foo"},
			expectedRemoved:            map[string][]byte{"foo": []byte(`"bar"`)},
			expectedDst:                []byte(`{"foo1":"bar1"}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "two items, string item at the start",
			src:                        []byte(`{"foo1":"bar1","foo":"bar"}`),
			remove:                     []string{"foo1"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`"bar1"`)},
			expectedDst:                []byte(`{"foo":"bar"}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "remove multiple items. empty dst",
			src:                        []byte(`{"foo1":"bar1","foo":"bar"}`),
			remove:                     []string{"foo1", "foo"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`"bar1"`), "foo": []byte(`"bar"`)},
			expectedDst:                []byte(`{}`),
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "remove multiple items. non-empty dst",
			src:                        []byte(`{"foo1":"bar1","foo2":"bar2","foo":"bar","foo3":"bar3","foo4":"bar4","foo5":"bar5"}`),
			remove:                     []string{"foo1", "foo", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":"bar2","foo4":"bar4"}`),
			expectedRemoved:            map[string][]byte{"foo1": []byte(`"bar1"`), "foo": []byte(`"bar"`), "foo3": []byte(`"bar3"`), "foo5": []byte(`"bar5"`)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "key doesn't exist",
			src:                        []byte(`{"foo1":"bar1","foo2":"bar2","foo":"bar"}`),
			remove:                     []string{"foo", "foo1", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":"bar2"}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`"bar"`), "foo1": []byte(`"bar1"`)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "key to be removed exists as value",
			src:                        []byte(`{"foo4":"foo5","foo1":"bar1","foo2":"foo3","foo":"bar","foo6":"foo7"}`),
			remove:                     []string{"foo", "foo1", "foo3", "foo5", "foo7"},
			expectedDst:                []byte(`{"foo4":"foo5","foo2":"foo3","foo6":"foo7"}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`"bar"`), "foo1": []byte(`"bar1"`)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:   "not a JSON object, but a string",
			src:    []byte("string"),
			remove: []string{"string"},
			fails:  true,
		},
		{
			name:   "not a JSON object, but a list",
			src:    []byte(`["string",1,2]`),
			remove: []string{"string"},
			fails:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Printf("======= %s ========\n", tt.name)
			dst := make([]byte, len(tt.src))
			removed := make(map[string][]byte)
			length, removedCnt, atleastOneField, err := MatchAndRemoveItemsFromJsonObject([]byte(tt.src), tt.remove, dst, removed)
			dst = dst[:length]
			if tt.fails {
				a.NotNil(err)
			} else {
				a.Nil(err)
			}
			a.Equal(removedCnt, len(tt.expectedRemoved))
			a.NotNil(dst)
			a.Equal(length, len(dst))
			a.Equal(len(removed), len(tt.expectedRemoved))
			a.Equal(atleastOneField, tt.expectedAtleastOnFieldLeft)
			a.Equal(bytes.Equal(dst, []byte(tt.expectedDst)), true)
			for k, v := range tt.expectedRemoved {
				a.Equal(string(removed[k]), string(v))
			}
		})
	}
}

func Test_RemoveEscapeString(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name                       string
		src, expectedDst           []byte
		remove                     []string
		expectedRemoved            map[string][]byte
		expectedAtleastOnFieldLeft bool
	}{
		{
			name:                       "only one removed item",
			src:                        []byte(`{"foo":"b\"a\"r"}`),
			remove:                     []string{"foo"},
			expectedDst:                []byte(`{}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`"b\"a\"r"`)},
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "two items, removed item at the end",
			src:                        []byte(`{"foo1":"b\"a\"r1","foo":"b\"a\"r"}`),
			remove:                     []string{"foo"},
			expectedRemoved:            map[string][]byte{"foo": []byte(`"b\"a\"r"`)},
			expectedDst:                []byte(`{"foo1":"b\"a\"r1"}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "two items, removed item at the start",
			src:                        []byte(`{"foo1":"b\"a\"r1","foo":"b\"a\"r"}`),
			remove:                     []string{"foo1"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`"b\"a\"r1"`)},
			expectedDst:                []byte(`{"foo":"b\"a\"r"}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "remove multiple items. empty dst",
			src:                        []byte(`{"foo1":"b\"a\"r1","foo":"b\"a\"r"}`),
			remove:                     []string{"foo1", "foo"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`"b\"a\"r1"`), "foo": []byte(`"b\"a\"r"`)},
			expectedDst:                []byte(`{}`),
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "remove multiple items. non-empty dst",
			src:                        []byte(`{"foo1":"b\"a\"r1","foo2":"b\"a\"r2","foo":"b\"a\"r","foo3":"b\"a\"r3","foo4":"b\"a\"r4","foo5":"b\"a\"r5"}`),
			remove:                     []string{"foo1", "foo", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":"b\"a\"r2","foo4":"b\"a\"r4"}`),
			expectedRemoved:            map[string][]byte{"foo1": []byte(`"b\"a\"r1"`), "foo": []byte(`"b\"a\"r"`), "foo3": []byte(`"b\"a\"r3"`), "foo5": []byte(`"b\"a\"r5"`)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "key doesn't exist",
			src:                        []byte(`{"foo1":"b\"a\"r1","foo2":"b\"a\"r2","foo":"b\"a\"r"}`),
			remove:                     []string{"foo", "foo1", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":"b\"a\"r2"}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`"b\"a\"r"`), "foo1": []byte(`"b\"a\"r1"`)},
			expectedAtleastOnFieldLeft: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Printf("======= %s ========\n", tt.name)
			dst := make([]byte, len(tt.src))
			removed := make(map[string][]byte)
			length, removedCnt, atleastOneField, err := MatchAndRemoveItemsFromJsonObject([]byte(tt.src), tt.remove, dst, removed)
			dst = dst[:length]
			a.Nil(err)
			a.Equal(removedCnt, len(tt.expectedRemoved))
			a.NotNil(dst)
			a.Equal(length, len(dst))
			a.Equal(len(removed), len(tt.expectedRemoved))
			a.Equal(atleastOneField, tt.expectedAtleastOnFieldLeft)
			a.Equal(bytes.Equal(dst, []byte(tt.expectedDst)), true)
			for k, v := range tt.expectedRemoved {
				a.Equal(string(removed[k]), string(v))
			}
		})
	}
}

func Test_RemoveInteger(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name                       string
		src, expectedDst           []byte
		remove                     []string
		expectedRemoved            map[string][]byte
		expectedAtleastOnFieldLeft bool
	}{
		{
			name:                       "only one removed item",
			src:                        []byte(`{"foo":12345}`),
			remove:                     []string{"foo"},
			expectedDst:                []byte(`{}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`12345`)},
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "two items, removed item at the end",
			src:                        []byte(`{"foo1":12345,"foo":12346}`),
			remove:                     []string{"foo"},
			expectedRemoved:            map[string][]byte{"foo": []byte(`12346`)},
			expectedDst:                []byte(`{"foo1":12345}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "two items, removed item at the start",
			src:                        []byte(`{"foo1":12345,"foo":123456}`),
			remove:                     []string{"foo1"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`12345`)},
			expectedDst:                []byte(`{"foo":123456}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "remove multiple items. empty dst",
			src:                        []byte(`{"foo1":123456,"foo":12345}`),
			remove:                     []string{"foo1", "foo"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`123456`), "foo": []byte(`12345`)},
			expectedDst:                []byte(`{}`),
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "remove multiple items. non-empty dst",
			src:                        []byte(`{"foo1":123451,"foo2":123452,"foo":12345,"foo3":123453,"foo4":123454,"foo5":123455}`),
			remove:                     []string{"foo1", "foo", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":123452,"foo4":123454}`),
			expectedRemoved:            map[string][]byte{"foo1": []byte(`123451`), "foo": []byte(`12345`), "foo3": []byte(`123453`), "foo5": []byte(`123455`)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "key doesn't exist",
			src:                        []byte(`{"foo1":123451,"foo2":123452,"foo":12345}`),
			remove:                     []string{"foo", "foo1", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":123452}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`12345`), "foo1": []byte(`123451`)},
			expectedAtleastOnFieldLeft: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Printf("======= %s ========\n", tt.name)
			dst := make([]byte, len(tt.src))
			removed := make(map[string][]byte)
			length, removedCnt, atleastOneField, err := MatchAndRemoveItemsFromJsonObject([]byte(tt.src), tt.remove, dst, removed)
			dst = dst[:length]
			a.Nil(err)
			a.Equal(removedCnt, len(tt.expectedRemoved))
			a.NotNil(dst)
			a.Equal(length, len(dst))
			a.Equal(len(removed), len(tt.expectedRemoved))
			a.Equal(atleastOneField, tt.expectedAtleastOnFieldLeft)
			a.Equal(bytes.Equal(dst, []byte(tt.expectedDst)), true)
			for k, v := range tt.expectedRemoved {
				a.Equal(string(removed[k]), string(v))
			}
		})
	}
}

func Test_RemoveNumber(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name                       string
		src, expectedDst           []byte
		remove                     []string
		expectedRemoved            map[string][]byte
		expectedAtleastOnFieldLeft bool
	}{
		{
			name:                       "only one removed item",
			src:                        []byte(`{"foo":3.142}`),
			remove:                     []string{"foo"},
			expectedDst:                []byte(`{}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`3.142`)},
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "two items, removed item at the end",
			src:                        []byte(`{"foo1":3.1421,"foo":3.142}`),
			remove:                     []string{"foo"},
			expectedRemoved:            map[string][]byte{"foo": []byte(`3.142`)},
			expectedDst:                []byte(`{"foo1":3.1421}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "two items, removed item at the start",
			src:                        []byte(`{"foo1":3.1421,"foo":3.142}`),
			remove:                     []string{"foo1"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`3.1421`)},
			expectedDst:                []byte(`{"foo":3.142}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "remove multiple items. empty dst",
			src:                        []byte(`{"foo1":3.1421,"foo":3.142}`),
			remove:                     []string{"foo1", "foo"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`3.1421`), "foo": []byte(`3.142`)},
			expectedDst:                []byte(`{}`),
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "remove multiple items. non-empty dst",
			src:                        []byte(`{"foo1":3.1421,"foo2":3.1422,"foo":3.142,"foo3":3.1423,"foo4":3.1424,"foo5":3.1425}`),
			remove:                     []string{"foo1", "foo", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":3.1422,"foo4":3.1424}`),
			expectedRemoved:            map[string][]byte{"foo1": []byte(`3.1421`), "foo": []byte(`3.142`), "foo3": []byte(`3.1423`), "foo5": []byte(`3.1425`)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "key doesn't exist",
			src:                        []byte(`{"foo1":3.1421,"foo2":3.1422,"foo":3.142}`),
			remove:                     []string{"foo", "foo1", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":3.1422}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`3.142`), "foo1": []byte(`3.1421`)},
			expectedAtleastOnFieldLeft: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Printf("======= %s ========\n", tt.name)
			dst := make([]byte, len(tt.src))
			removed := make(map[string][]byte)
			length, removedCnt, atleastOneField, err := MatchAndRemoveItemsFromJsonObject([]byte(tt.src), tt.remove, dst, removed)
			dst = dst[:length]
			a.Nil(err)
			a.Equal(removedCnt, len(tt.expectedRemoved))
			a.NotNil(dst)
			a.Equal(length, len(dst))
			a.Equal(len(removed), len(tt.expectedRemoved))
			a.Equal(atleastOneField, tt.expectedAtleastOnFieldLeft)
			a.Equal(bytes.Equal(dst, []byte(tt.expectedDst)), true)
			for k, v := range tt.expectedRemoved {
				a.Equal(string(removed[k]), string(v))
			}
		})
	}
}

func Test_RemoveNull(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name                       string
		src, expectedDst           []byte
		remove                     []string
		expectedRemoved            map[string][]byte
		expectedAtleastOnFieldLeft bool
	}{
		{
			name:                       "only one removed item",
			src:                        []byte(`{"foo":null}`),
			remove:                     []string{"foo"},
			expectedDst:                []byte(`{}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`null`)},
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "two items, removed item at the end",
			src:                        []byte(`{"foo1":null,"foo":null}`),
			remove:                     []string{"foo"},
			expectedRemoved:            map[string][]byte{"foo": []byte(`null`)},
			expectedDst:                []byte(`{"foo1":null}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "two items, removed item at the start",
			src:                        []byte(`{"foo1":null,"foo":null}`),
			remove:                     []string{"foo1"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`null`)},
			expectedDst:                []byte(`{"foo":null}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "remove multiple items. empty dst",
			src:                        []byte(`{"foo1":null,"foo":null}`),
			remove:                     []string{"foo1", "foo"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`null`), "foo": []byte(`null`)},
			expectedDst:                []byte(`{}`),
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "remove multiple items. non-empty dst",
			src:                        []byte(`{"foo1":null,"foo2":null,"foo":null,"foo3":null,"foo4":null,"foo5":null}`),
			remove:                     []string{"foo1", "foo", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":null,"foo4":null}`),
			expectedRemoved:            map[string][]byte{"foo1": []byte(`null`), "foo": []byte(`null`), "foo3": []byte(`null`), "foo5": []byte(`null`)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "key doesn't exist",
			src:                        []byte(`{"foo1":null,"foo2":null,"foo":null}`),
			remove:                     []string{"foo", "foo1", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":null}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`null`), "foo1": []byte(`null`)},
			expectedAtleastOnFieldLeft: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Printf("======= %s ========\n", tt.name)
			dst := make([]byte, len(tt.src))
			removed := make(map[string][]byte)
			length, removedCnt, atleastOneField, err := MatchAndRemoveItemsFromJsonObject([]byte(tt.src), tt.remove, dst, removed)
			dst = dst[:length]
			a.Nil(err)
			a.Equal(removedCnt, len(tt.expectedRemoved))
			a.NotNil(dst)
			a.Equal(length, len(dst))
			a.Equal(len(removed), len(tt.expectedRemoved))
			a.Equal(atleastOneField, tt.expectedAtleastOnFieldLeft)
			a.Equal(bytes.Equal(dst, []byte(tt.expectedDst)), true)
			for k, v := range tt.expectedRemoved {
				a.Equal(string(removed[k]), string(v))
			}
		})
	}
}

func Test_RemoveBoolean(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name                       string
		src, expectedDst           []byte
		remove                     []string
		expectedRemoved            map[string][]byte
		expectedAtleastOnFieldLeft bool
	}{
		{
			name:                       "only one removed item - true",
			src:                        []byte(`{"foo":true}`),
			remove:                     []string{"foo"},
			expectedDst:                []byte(`{}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`true`)},
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "two items, removed item at the end - true",
			src:                        []byte(`{"foo1":true,"foo":true}`),
			remove:                     []string{"foo"},
			expectedRemoved:            map[string][]byte{"foo": []byte(`true`)},
			expectedDst:                []byte(`{"foo1":true}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "two items, removed item at the start - true",
			src:                        []byte(`{"foo1":true,"foo":true}`),
			remove:                     []string{"foo1"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`true`)},
			expectedDst:                []byte(`{"foo":true}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "remove multiple items. empty dst - true",
			src:                        []byte(`{"foo1":true,"foo":true}`),
			remove:                     []string{"foo1", "foo"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`true`), "foo": []byte(`true`)},
			expectedDst:                []byte(`{}`),
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "remove multiple items. non-empty dst - true",
			src:                        []byte(`{"foo1":true,"foo2":true,"foo":true,"foo3":true,"foo4":true,"foo5":true}`),
			remove:                     []string{"foo1", "foo", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":true,"foo4":true}`),
			expectedRemoved:            map[string][]byte{"foo1": []byte(`true`), "foo": []byte(`true`), "foo3": []byte(`true`), "foo5": []byte(`true`)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "key doesn't exist - true",
			src:                        []byte(`{"foo1":true,"foo2":true,"foo":true}`),
			remove:                     []string{"foo", "foo1", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":true}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`true`), "foo1": []byte(`true`)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "only one removed item - false",
			src:                        []byte(`{"foo":false}`),
			remove:                     []string{"foo"},
			expectedDst:                []byte(`{}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`false`)},
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "two items, removed item at the end - false",
			src:                        []byte(`{"foo1":false,"foo":false}`),
			remove:                     []string{"foo"},
			expectedRemoved:            map[string][]byte{"foo": []byte(`false`)},
			expectedDst:                []byte(`{"foo1":false}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "two items, removed item at the start - false",
			src:                        []byte(`{"foo1":false,"foo":false}`),
			remove:                     []string{"foo1"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`false`)},
			expectedDst:                []byte(`{"foo":false}`),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "remove multiple items. empty dst - false",
			src:                        []byte(`{"foo1":false,"foo":false}`),
			remove:                     []string{"foo1", "foo"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(`false`), "foo": []byte(`false`)},
			expectedDst:                []byte(`{}`),
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "remove multiple items. non-empty dst - false",
			src:                        []byte(`{"foo1":false,"foo2":false,"foo":false,"foo3":false,"foo4":false,"foo5":false}`),
			remove:                     []string{"foo1", "foo", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":false,"foo4":false}`),
			expectedRemoved:            map[string][]byte{"foo1": []byte(`false`), "foo": []byte(`false`), "foo3": []byte(`false`), "foo5": []byte(`false`)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "key doesn't exist - false",
			src:                        []byte(`{"foo1":false,"foo2":false,"foo":false}`),
			remove:                     []string{"foo", "foo1", "foo3", "foo5"},
			expectedDst:                []byte(`{"foo2":false}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(`false`), "foo1": []byte(`false`)},
			expectedAtleastOnFieldLeft: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Printf("======= %s ========\n", tt.name)
			dst := make([]byte, len(tt.src))
			removed := make(map[string][]byte)
			length, removedCnt, atleastOneField, err := MatchAndRemoveItemsFromJsonObject([]byte(tt.src), tt.remove, dst, removed)
			dst = dst[:length]
			a.Nil(err)
			a.Equal(removedCnt, len(tt.expectedRemoved))
			a.NotNil(dst)
			a.Equal(length, len(dst))
			a.Equal(len(removed), len(tt.expectedRemoved))
			a.Equal(atleastOneField, tt.expectedAtleastOnFieldLeft)
			a.Equal(bytes.Equal(dst, []byte(tt.expectedDst)), true)
			for k, v := range tt.expectedRemoved {
				a.Equal(string(removed[k]), string(v))
			}
		})
	}
}

func Test_RemoveObject(t *testing.T) {
	a := assert.New(t)

	obj0 := `{"obj0":{"o6":"hello"},"obj1":{},"obj2":{"obj3":"obj4","obj6":[1,2,"hello",4,"world",{"obj7":7}],"obj8":123,"obj9":null,"obj10":false},"obj11":{"5":null,"7":true,"6":3.142},"obj12":{"12":false},"obj13":{"34":1234,"4":569},"nested":"nested":{"bar1":"bar2","bar3":"bar4","bar5":"bar6","bar7":"bar8","bar9":"bar10"}}`
	obj1 := `{"obj0":{"o":{"o1":{}}},"obj1":{"o":[],"o1":{},"nested":"nested":{"bar1":"bar2","bar3":"bar4","bar5":"bar6","bar7":"bar8","bar9":"bar10"},"o2":[{},{"hi":"hello"}]},"obj2":{"obj3":false,"obj6":431,"obj8":null,"obj9":{"o":{}},"obj10":false},"obj12":{"pi":3.1421},"obj13":{"null":null},"obj11":{"list":[1,2,"hello",4,"world",{"obj7":7}]}}`
	obj2 := `{"nested":{"bar1":"bar2","bar3":"bar4","bar5":"bar6","bar7":"bar8","bar9":"bar10"},"obj8":{"num":123},"obj9":{"null":null,"null1":"null1"},"obj10":false},"obj11":{"LIST":"list","list":[5,6,7,3.142]},"obj12":{"pi":3.1421,"obj13":null,"obj0":false,"obj1":123},"obj2":{"obj3":"obj4","obj6":[1,2,"hello",4,"world",{"obj7":7}]}`
	obj3 := `{"obj1":{"num":123,"obj2":{"obj3":"obj4","obj6":[1,2,"hello",4,"world",{},{},{}],"obj0":false,"obj8":123,"obj9":null,"obj10":false}},"obj11":[5,6,7,3.142],"obj12":3.1421,"obj13":null}`
	obj4 := `{"obj0":false,"obj1":123,"obj11":[5,6,7,3.142],"obj12":3.1421,"obj2":{"obj3":"obj4","obj6":[1,2,"hello",4,"world",{"obj7":7}],"obj8":123,"obj9":null,"obj10":false},"obj13":null}`
	obj5 := `{"obj0":false,"obj1":123,"obj2":{"obj3":"obj4","obj6":[1,2,"hello",4,"world",{"obj7":7}],"obj8":123,"obj9":null,"obj10":false},"obj11":[5,6,7,3.142],"obj12":3.1421,"obj13":null}`

	tests := []struct {
		name                       string
		src, expectedDst           []byte
		remove                     []string
		expectedRemoved            map[string][]byte
		expectedAtleastOnFieldLeft bool
	}{
		{
			name:                       "only one removed item",
			src:                        []byte(fmt.Sprintf(`{"foo":%v}`, obj0)),
			remove:                     []string{"foo"},
			expectedDst:                []byte(`{}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(obj0)},
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "two items, removed item at the end",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo":%v}`, obj1, obj0)),
			remove:                     []string{"foo"},
			expectedRemoved:            map[string][]byte{"foo": []byte(obj0)},
			expectedDst:                []byte(fmt.Sprintf(`{"foo1":%v}`, obj1)),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "two items, removed item at the start",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo":%v}`, obj1, obj0)),
			remove:                     []string{"foo1"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(obj1)},
			expectedDst:                []byte(fmt.Sprintf(`{"foo":%v}`, obj0)),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "remove multiple items. empty dst",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo":%v}`, obj1, obj0)),
			remove:                     []string{"foo1", "foo"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(obj1), "foo": []byte(obj0)},
			expectedDst:                []byte(`{}`),
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "remove multiple items. non-empty dst",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo2":%v,"foo":%v,"foo3":%v,"foo4":%v,"foo5":%v}`, obj1, obj2, obj0, obj3, obj4, obj5)),
			remove:                     []string{"foo1", "foo", "foo3", "foo5"},
			expectedDst:                []byte(fmt.Sprintf(`{"foo2":%v,"foo4":%v}`, obj2, obj4)),
			expectedRemoved:            map[string][]byte{"foo1": []byte(obj1), "foo": []byte(obj0), "foo3": []byte(obj3), "foo5": []byte(obj5)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "key doesn't exist",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo2":%v,"foo":%v}`, obj1, obj2, obj0)),
			remove:                     []string{"foo", "foo1", "foo3", "foo5"},
			expectedDst:                []byte(fmt.Sprintf(`{"foo2":%v}`, obj2)),
			expectedRemoved:            map[string][]byte{"foo": []byte(obj0), "foo1": []byte(obj1)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "to remove string is a value",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo2":%v,"foo":%v}`, obj1, obj2, obj0)),
			remove:                     []string{"foo", "foo1", "foo3", "foo5", "bar2", "bar4", "bar6", "bar8", "bar10"},
			expectedDst:                []byte(fmt.Sprintf(`{"foo2":%v}`, obj2)),
			expectedRemoved:            map[string][]byte{"foo": []byte(obj0), "foo1": []byte(obj1)},
			expectedAtleastOnFieldLeft: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Printf("======= %s ========\n", tt.name)
			dst := make([]byte, len(tt.src))
			removed := make(map[string][]byte)
			length, removedCnt, atleastOneField, err := MatchAndRemoveItemsFromJsonObject([]byte(tt.src), tt.remove, dst, removed)
			dst = dst[:length]
			a.Nil(err)
			a.Equal(removedCnt, len(tt.expectedRemoved))
			a.NotNil(dst)
			a.Equal(length, len(dst))
			a.Equal(len(removed), len(tt.expectedRemoved))
			a.Equal(atleastOneField, tt.expectedAtleastOnFieldLeft)
			a.Equal(bytes.Equal(dst, []byte(tt.expectedDst)), true)
			for k, v := range tt.expectedRemoved {
				a.Equal(string(removed[k]), string(v))
			}
		})
	}
}

func Test_RemoveArray(t *testing.T) {
	a := assert.New(t)

	obj0 := `[["hi",{},null,true,false,{"hello":"world"}],{},true,{"obj0":{"o6":"hello"},"obj1":{},"obj2":{"obj3":"obj4","obj6":[1,2,"hello",4,"world",{"obj7":7}],"obj8":123,"obj9":null,"obj10":false},"obj11":{"5":null,"7":true,"6":3.142},"obj12":{"12":false},"obj13":{"34":1234,"4":569}},false,3.142,"h\"i"]`
	obj1 := `[false,3.142,[],234,"helloWorld",[],[],[],[[],[],[],{},[],{}],{"obj0":{"o":{"o1":{}}},"obj1":{"o":[],"o1":{},"o2":[{},{"hi":"hello"}]},"obj2":{"obj3":false,"obj6":431,"obj8":null,"obj9":{"o":{}},"obj10":false},"obj12":{"pi":3.1421},"obj13":{"null":null},"obj11":{"list":[1,2,"hello",4,"world",{"obj7":7}]}},null}`
	obj2 := `["\"foo\"",{"hi":"hello"},123,{"obj8":{"num":123},"obj9":{"null":null,"null1":"null1"},"obj10":false},"obj11":{"LIST":"list","list":[5,6,7,3.142]},"obj12":{"pi":3.1421,"obj13":null,"obj0":false,"obj1":123},"obj2":{"obj3":"obj4","obj6":[1,2,"hello",4,"world",{"obj7":7}]},null,null,true,false]`
	obj3 := `[3.123,null,false,false,true,[],[1,23,"hi"],{},{}{"obj1":{"num":123,"obj2":{"obj3":"obj4","obj6":[1,2,"hello",4,"world",{},{},{}],"obj0":false,"obj8":123,"obj9":null,"obj10":false}},"obj11":[5,6,7,3.142],"obj12":3.1421,"obj13":null},123]`
	obj4 := `[{"obj0":false,"obj1":123,"obj11":[5,6,7,3.142],"obj12":3.1421,"obj2":{"obj3":"obj4","obj6":[1,2,"hello",4,"world",{"obj7":7}],"obj8":123,"obj9":null,"obj10":false},"obj13":null},3.132,123,false,"\"\"",true,{},[],[1,2,3]]`
	obj5 := `[[],"\"\""[1,"hi"],{},{"foo":"bar"},true,false,null{"obj0":false,"obj1":123,"obj2":{"obj3":"obj4","obj6":[1,2,"hello",4,"world",{"obj7":7}],"obj8":123,"obj9":null,"obj10":false},"obj11":[5,6,7,3.142],"obj12":3.1421,"obj13":null}]`

	tests := []struct {
		name                       string
		src, expectedDst           []byte
		remove                     []string
		expectedRemoved            map[string][]byte
		expectedAtleastOnFieldLeft bool
	}{
		{
			name:                       "only one removed item",
			src:                        []byte(fmt.Sprintf(`{"foo":%v}`, obj0)),
			remove:                     []string{"foo"},
			expectedDst:                []byte(`{}`),
			expectedRemoved:            map[string][]byte{"foo": []byte(obj0)},
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "two items, removed item at the end",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo":%v}`, obj1, obj0)),
			remove:                     []string{"foo"},
			expectedRemoved:            map[string][]byte{"foo": []byte(obj0)},
			expectedDst:                []byte(fmt.Sprintf(`{"foo1":%v}`, obj1)),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "two items, removed item at the start",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo":%v}`, obj1, obj0)),
			remove:                     []string{"foo1"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(obj1)},
			expectedDst:                []byte(fmt.Sprintf(`{"foo":%v}`, obj0)),
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "remove multiple items. empty dst",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo":%v}`, obj1, obj0)),
			remove:                     []string{"foo1", "foo"},
			expectedRemoved:            map[string][]byte{"foo1": []byte(obj1), "foo": []byte(obj0)},
			expectedDst:                []byte(`{}`),
			expectedAtleastOnFieldLeft: false,
		},
		{
			name:                       "remove multiple items. non-empty dst",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo2":%v,"foo":%v,"foo3":%v,"foo4":%v,"foo5":%v}`, obj1, obj2, obj0, obj3, obj4, obj5)),
			remove:                     []string{"foo1", "foo", "foo3", "foo5"},
			expectedDst:                []byte(fmt.Sprintf(`{"foo2":%v,"foo4":%v}`, obj2, obj4)),
			expectedRemoved:            map[string][]byte{"foo1": []byte(obj1), "foo": []byte(obj0), "foo3": []byte(obj3), "foo5": []byte(obj5)},
			expectedAtleastOnFieldLeft: true,
		},
		{
			name:                       "key doesn't exist",
			src:                        []byte(fmt.Sprintf(`{"foo1":%v,"foo2":%v,"foo":%v}`, obj1, obj2, obj0)),
			remove:                     []string{"foo", "foo1", "foo3", "foo5"},
			expectedDst:                []byte(fmt.Sprintf(`{"foo2":%v}`, obj2)),
			expectedRemoved:            map[string][]byte{"foo": []byte(obj0), "foo1": []byte(obj1)},
			expectedAtleastOnFieldLeft: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Printf("======= %s ========\n", tt.name)
			dst := make([]byte, len(tt.src))
			removed := make(map[string][]byte)
			length, removedCnt, atleastOneField, err := MatchAndRemoveItemsFromJsonObject([]byte(tt.src), tt.remove, dst, removed)
			dst = dst[:length]
			a.Nil(err)
			a.Equal(removedCnt, len(tt.expectedRemoved))
			a.NotNil(dst)
			a.Equal(length, len(dst))
			a.Equal(len(removed), len(tt.expectedRemoved))
			a.Equal(atleastOneField, tt.expectedAtleastOnFieldLeft)
			a.Equal(bytes.Equal(dst, []byte(tt.expectedDst)), true)
			for k, v := range tt.expectedRemoved {
				a.Equal(string(removed[k]), string(v))
			}
		})
	}
}

func Test_RemoveNestedItem(t *testing.T) {
	a := assert.New(t)

	stringValues := []string{`"bar"`, `"bar1"`, `"bar2"`}
	escapeStringValues := []string{`"\"b\"ar"`, `"b\"a\"r1"`, `"ba\"r\"2"`}
	trueValues := []string{"true", "true", "true"}
	falseValues := []string{"false", "false", "false"}
	intValues := []string{"123", "4562", "3142"}
	numValues := []string{"3.142", "9.81", "10.00"}
	nullValues := []string{"null", "null", "null"}
	objectValues := []string{
		`{"a":{"b":[1,2,3],"c":"d"},"f":null,"g":[{},{"hi":"hello"}]}`,
		`{"f":"foo","a":{"b":[1,2,3],"c":"d"},"g":[{},{"hi":"hello"}]}`,
		`{"g":[{},{"hi":"hello"},"b\"a\"r"],"a":{"b":[1,2,3],"c":"d"},"f":3.142}`,
	}
	listValues := []string{
		`[{"b":[[],{},[]],"c":"d"},["hi",{},3.142,3,null,{"hi":"hello"},true],"str","\"s"]`,
		`[["hi",{},3.142,3,null,{"hi":"hello"},true],"num":3.142,{"b":[[],{},[]],"c":"d"}]`,
		`["bool":true,{"b":[[],{},[]],"c":"d"},["hi",{},3.142,3,null,{"hi":"hello"},true]]`,
	}

	values := [][]string{stringValues, escapeStringValues, trueValues, falseValues, intValues, nullValues, numValues, objectValues, listValues}
	types := []string{"string", "escape string", "true boolean", "false boolean", "integer", "null", "number", "object", "list"}

	for i, value := range values {
		tests := []struct {
			name                       string
			src, expectedDst           []byte
			remove                     []string
			expectedRemoved            map[string][]byte
			expectedAtleastOnFieldLeft bool
		}{
			{
				name:                       fmt.Sprintf("only nested value - %v", types[i]),
				src:                        []byte(fmt.Sprintf(`{"obj1":{"foo":%v}}`, value[0])),
				expectedDst:                []byte(`{"obj1":{}}`),
				remove:                     []string{"foo"},
				expectedRemoved:            map[string][]byte{"foo": []byte(value[0])},
				expectedAtleastOnFieldLeft: true,
			},
			{
				name:                       fmt.Sprintf("nested value at the beginning - %v", types[i]),
				src:                        []byte(fmt.Sprintf(`{"obj1":{"foo":%v,"foo1":%v}}`, value[0], value[1])),
				expectedDst:                []byte(fmt.Sprintf(`{"obj1":{"foo1":%v}}`, value[1])),
				remove:                     []string{"foo"},
				expectedRemoved:            map[string][]byte{"foo": []byte(value[0])},
				expectedAtleastOnFieldLeft: true,
			},
			{
				name:                       fmt.Sprintf("nested value at the end - %v", types[i]),
				src:                        []byte(fmt.Sprintf(`{"obj1":{"foo":%v,"foo1":%v}}`, value[0], value[1])),
				expectedDst:                []byte(fmt.Sprintf(`{"obj1":{"foo":%v}}`, value[0])),
				remove:                     []string{"foo1"},
				expectedRemoved:            map[string][]byte{"foo1": []byte(value[1])},
				expectedAtleastOnFieldLeft: true,
			},
			{
				name:                       fmt.Sprintf("nested value at the middle - %v", types[i]),
				src:                        []byte(fmt.Sprintf(`{"obj1":{"foo":%v,"foo1":%v,"foo2":%v}}`, value[0], value[1], value[2])),
				expectedDst:                []byte(fmt.Sprintf(`{"obj1":{"foo":%v,"foo2":%v}}`, value[0], value[2])),
				remove:                     []string{"foo1"},
				expectedRemoved:            map[string][]byte{"foo1": []byte(value[1])},
				expectedAtleastOnFieldLeft: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				fmt.Printf("======= %s ========\n", tt.name)
				dst := make([]byte, len(tt.src))
				removed := make(map[string][]byte)
				length, removedCnt, atleastOneField, err := MatchAndRemoveItemsFromJsonObject([]byte(tt.src), tt.remove, dst, removed)
				dst = dst[:length]
				a.Nil(err)
				a.Equal(removedCnt, len(tt.expectedRemoved))
				a.NotNil(dst)
				a.Equal(length, len(dst))
				a.Equal(len(removed), len(tt.expectedRemoved))
				a.Equal(atleastOneField, tt.expectedAtleastOnFieldLeft)
				a.Equal(bytes.Equal(dst, []byte(tt.expectedDst)), true)
				for k, v := range tt.expectedRemoved {
					a.Equal(string(removed[k]), string(v))
				}
			})
		}
	}
}
