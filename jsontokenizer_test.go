package gojsonsm

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

func testTokenizedStep(t *testing.T, tok *jsonTokenizer, expectedToken tokenType, expectedTokenStr string) {
	t.Helper()

	token, tokenData, _, err := tok.Step()
	if err != nil {
		t.Fatalf("encountered stepping error: %s", err)
	}

	if token != expectedToken {
		t.Fatalf("Expected token `%s` but got `%s`", tokenToText(expectedToken), tokenToText(token))
	}

	if bytes.Compare(tokenData, []byte(expectedTokenStr)) != 0 {
		t.Fatalf("Expected token data `%s` but got `%s`", expectedTokenStr, tokenData)
	}
}

func testTokenizedValue(t *testing.T, dataBytes []byte, token tokenType, tokenData string) {
	t.Helper()

	// Set up the tokenizer
	var tok jsonTokenizer
	tok.Reset(dataBytes)

	// Check that we receive the correct token
	testTokenizedStep(t, &tok, token, tokenData)

	// Check that we get end after the token
	testTokenizedStep(t, &tok, tknEnd, "")
}

func testTokenizedValueEx(t *testing.T, data string, token tokenType) {
	t.Helper()

	dataBytes := []byte(data)
	var testValue []byte

	WHITESPACEBYTES := []byte("  \t  \n ")

	// Check normally
	testValue = dataBytes
	testTokenizedValue(t, testValue, token, data)

	// Check with whitespace ahead of it
	testValue = append(WHITESPACEBYTES, dataBytes...)
	testTokenizedValue(t, testValue, token, data)

	// Check with whitespace after of it
	testValue = append(dataBytes, WHITESPACEBYTES...)
	testTokenizedValue(t, testValue, token, data)
}

func TestTokenizerSeeking(t *testing.T) {
	dataBytes := []byte(`{
		"a": "5b47eb0936ff92a567a0307e",
		"b": false
	}`)

	var tok jsonTokenizer
	tok.Reset(dataBytes)

	testTokenizedStep(t, &tok, tknObjectStart, "{")
	testTokenizedStep(t, &tok, tknString, `"a"`)

	savedPos := tok.Position()
	testTokenizedStep(t, &tok, tknObjectKeyDelim, ":")

	tok.Seek(savedPos)
	testTokenizedStep(t, &tok, tknObjectKeyDelim, ":")
	testTokenizedStep(t, &tok, tknString, `"5b47eb0936ff92a567a0307e"`)
}

func TestTokenizeObject(t *testing.T) {
	dataBytes := []byte(`{
		"a": "5b47eb0936ff92a567a0307e",
		"b": false
	}`)

	var tok jsonTokenizer
	tok.Reset(dataBytes)

	testTokenizedStep(t, &tok, tknObjectStart, "{")
	testTokenizedStep(t, &tok, tknString, `"a"`)
	testTokenizedStep(t, &tok, tknObjectKeyDelim, ":")
	testTokenizedStep(t, &tok, tknString, `"5b47eb0936ff92a567a0307e"`)
	testTokenizedStep(t, &tok, tknListDelim, ",")
	testTokenizedStep(t, &tok, tknString, `"b"`)
	testTokenizedStep(t, &tok, tknObjectKeyDelim, ":")
	testTokenizedStep(t, &tok, tknFalse, `false`)
	testTokenizedStep(t, &tok, tknObjectEnd, "}")
	testTokenizedStep(t, &tok, tknEnd, "")
}

func TestTokenizeArray(t *testing.T) {
	dataBytes := []byte(`[
		1,
		2999.22,
		null,
		"hello\u2932world"
	]`)

	var tok jsonTokenizer
	tok.Reset(dataBytes)

	testTokenizedStep(t, &tok, tknArrayStart, "[")
	testTokenizedStep(t, &tok, tknInteger, "1")
	testTokenizedStep(t, &tok, tknListDelim, ",")
	testTokenizedStep(t, &tok, tknNumber, "2999.22")
	testTokenizedStep(t, &tok, tknListDelim, ",")
	testTokenizedStep(t, &tok, tknNull, "null")
	testTokenizedStep(t, &tok, tknListDelim, ",")
	testTokenizedStep(t, &tok, tknEscString, `"hello\u2932world"`)
	testTokenizedStep(t, &tok, tknArrayEnd, "]")
	testTokenizedStep(t, &tok, tknEnd, "")
}

func TestTokenizeString(t *testing.T) {
	testTokenizedValueEx(t, `"lol"`, tknString)
}

func TestTokenizeEscString(t *testing.T) {
	testTokenizedValueEx(t, `"l\nol"`, tknEscString)
	testTokenizedValueEx(t, `"l\u2321ol"`, tknEscString)
}

func TestTokenizeInteger(t *testing.T) {
	testTokenizedValueEx(t, `0`, tknInteger)
	testTokenizedValueEx(t, `123`, tknInteger)
	testTokenizedValueEx(t, `4565464651846548`, tknInteger)
}

func TestTokenizeNumber(t *testing.T) {
	testTokenizedValueEx(t, `0.1`, tknNumber)
	testTokenizedValueEx(t, `1999.1`, tknNumber)
	testTokenizedValueEx(t, `14.29438383`, tknNumber)
	testTokenizedValueEx(t, `1.0E+2`, tknNumber)
	testTokenizedValueEx(t, `1.9e+22`, tknNumber)
}

func TestTokenizeBool(t *testing.T) {
	testTokenizedValueEx(t, `true`, tknTrue)
	testTokenizedValueEx(t, `false`, tknFalse)
}

func TestTokenizeNull(t *testing.T) {
	testTokenizedValueEx(t, `null`, tknNull)
}

func TestTokenizeEndsForever(t *testing.T) {
	dataBytes := []byte(`"hello world"`)

	var tok jsonTokenizer
	tok.Reset(dataBytes)

	testTokenizedStep(t, &tok, tknString, `"hello world"`)
	testTokenizedStep(t, &tok, tknEnd, "")
	testTokenizedStep(t, &tok, tknEnd, "")
}

func TestTokenizerLong(t *testing.T) {
	dataBytes, err := ioutil.ReadFile("testdata/people.json")
	if err != nil {
		panic(fmt.Sprintf("failed to read test data file: %s", err))
	}

	var tok jsonTokenizer
	tok.Reset(dataBytes)
	for {
		token, _, _, err := tok.Step()
		if err != nil {
			panic(fmt.Sprintf("encountered stepping error: %s", err))
		}

		if token == tknEnd {
			break
		}
	}
}

func BenchmarkTokenize(b *testing.B) {
	var tok jsonTokenizer

	dataBytes, err := ioutil.ReadFile("testdata/people.json")
	if err != nil {
		panic(fmt.Sprintf("failed to read test data file: %s", err))
	}

	b.SetBytes(int64(len(dataBytes)))
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		tok.Reset(dataBytes)

		for {
			token, _, _, err := tok.Step()
			if err != nil {
				panic(fmt.Sprintf("encountered stepping error: %s", err))
			}

			if token == tknEnd {
				break
			}
		}
	}
}
