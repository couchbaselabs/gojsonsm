// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import "strings"

func reindentString(value string, indent string) string {
	return indent + strings.Replace(value, "\n", "\n"+indent, -1)
}

func DeepCopyStringArray(in []string) []string {
	if in == nil {
		return nil
	}

	out := make([]string, len(in))
	copy(out, in)
	return out
}
