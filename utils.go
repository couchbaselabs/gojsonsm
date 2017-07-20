package gojsonsm

import "strings"

func reindentString(value string, indent string) string {
	return indent + strings.Replace(value, "\n", "\n"+indent, -1)
}
