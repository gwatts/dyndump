// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"bytes"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	binAttr    = &dynamodb.AttributeValue{B: []byte("binary value")}
	boolAttr   = &dynamodb.AttributeValue{BOOL: aws.Bool(true)}
	binSetAttr = &dynamodb.AttributeValue{BS: [][]byte{[]byte("bin val1"), []byte("bin val2")}}
	strAttr    = &dynamodb.AttributeValue{S: aws.String("string value")}
	numAttr    = &dynamodb.AttributeValue{N: aws.String("123.456")}
	numSetAttr = &dynamodb.AttributeValue{NS: []*string{aws.String("100"), aws.String("200")}}
	nullAttr   = &dynamodb.AttributeValue{NULL: aws.Bool(true)}
	strSetAttr = &dynamodb.AttributeValue{SS: []*string{aws.String("str val1"), aws.String("str val2")}}
	listAttr   = &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{strAttr, numAttr}}
	mapAttr    = &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
		"key1": strAttr,
		"key2": numAttr,
	}}
)

var jsTests = []struct {
	name             string
	attr             *dynamodb.AttributeValue
	encodeTypes      bool
	numbersAsStrings bool
	expected         string
}{
	{"bin", binAttr, false, false, `{"key":"binary value"}`},
	{"bin-type", binAttr, true, false, `{"key":{"type":"binary","value":"binary value"}}`},
	{"bool", boolAttr, false, false, `{"key":true}`},
	{"bool-type", boolAttr, true, false, `{"key":{"type":"bool","value":true}}`},
	{"bin-set", binSetAttr, false, false, `{"key":["bin val1","bin val2"]}`},
	{"bin-set-type", binSetAttr, true, false, `{"key":{"type":"binary-set","value":["bin val1","bin val2"]}}`},
	{"string", strAttr, false, false, `{"key":"string value"}`},
	{"string-type", strAttr, true, false, `{"key":{"type":"string","value":"string value"}}`},
	{"num", numAttr, false, false, `{"key":123.456}`},
	{"num-type", numAttr, true, false, `{"key":{"type":"number","value":123.456}}`},
	{"num-asstring", numAttr, false, true, `{"key":"123.456"}`},
	{"num-type-asstring", numAttr, true, true, `{"key":{"type":"number","value":"123.456"}}`},
	{"num-set", numSetAttr, false, false, `{"key":[100,200]}`},
	{"num-set-asstring", numSetAttr, false, true, `{"key":["100","200"]}`},
	{"num-set-type-asstring", numSetAttr, true, true, `{"key":{"type":"number-set","value":["100","200"]}}`},
	{"null", nullAttr, false, false, `{"key":null}`},
	{"null-type", nullAttr, true, false, `{"key":{"type":"null","value":null}}`},
	{"str-set", strSetAttr, false, false, `{"key":["str val1","str val2"]}`},
	{"str-set-type", strSetAttr, true, false, `{"key":{"type":"string-set","value":["str val1","str val2"]}}`},
	{"list", listAttr, false, false, `{"key":["string value",123.456]}`},
	{"list-type", listAttr, true, false, `{"key":{"type":"list","value":[{"type":"string","value":"string value"},{"type":"number","value":123.456}]}}`},
	{"list-type-asstring", listAttr, true, true, `{"key":{"type":"list","value":[{"type":"string","value":"string value"},{"type":"number","value":"123.456"}]}}`},
	{"map", mapAttr, false, false, `{"key":{"key1":"string value","key2":123.456}}`},
	{"map-type", mapAttr, true, false, `{"key":{"type":"map","value":{"key1":{"type":"string","value":"string value"},"key2":{"type":"number","value":123.456}}}}`},
}

func TestJSONEncoder(t *testing.T) {
	for _, test := range jsTests {
		buf := &bytes.Buffer{}
		enc := NewJSONItemEncoder(buf, test.encodeTypes, test.numbersAsStrings)
		enc.WriteItem(map[string]*dynamodb.AttributeValue{"key": test.attr})
		val := strings.TrimSpace(buf.String())
		if val != test.expected {
			t.Errorf("test=%s expected=%q actual=%q", test.name, test.expected, val)
		}
	}
}
