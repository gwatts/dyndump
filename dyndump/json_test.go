// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var attrTests = []struct {
	name     string
	src      *dynamodb.AttributeValue
	expected string
}{
	{"bytes", &dynamodb.AttributeValue{B: []byte("foo")}, `{"k":{"B":"Zm9v"}}`},
	{"bool", &dynamodb.AttributeValue{BOOL: aws.Bool(true)}, `{"k":{"BOOL":true}}`},
	{"binary-set", &dynamodb.AttributeValue{BS: [][]byte{[]byte("foo"), []byte("bar")}}, `{"k":{"BS":["Zm9v","YmFy"]}}`},
	{"attr-list", &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
		{S: aws.String("str")},
		{BS: [][]byte{[]byte("foo"), []byte("bar")}},
	}}, `{"k":{"L":[{"S":"str"},{"BS":["Zm9v","YmFy"]}]}}`},
	{"attr-map", &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
		"key1": &dynamodb.AttributeValue{S: aws.String("str")},
		"key2": &dynamodb.AttributeValue{BS: [][]byte{[]byte("foo"), []byte("bar")}},
	}}, `{"k":{"M":{"key1":{"S":"str"},"key2":{"BS":["Zm9v","YmFy"]}}}}`},
	{"number", &dynamodb.AttributeValue{N: aws.String("123.456")}, `{"k":{"N":"123.456"}}`},
	{"number-set", &dynamodb.AttributeValue{NS: []*string{aws.String("123"), aws.String("456")}}, `{"k":{"NS":["123","456"]}}`},
	{"null", &dynamodb.AttributeValue{NULL: aws.Bool(true)}, `{"k":{"NULL":true}}`},
	{"string", &dynamodb.AttributeValue{S: aws.String("foo")}, `{"k":{"S":"foo"}}`},
	{"string-set", &dynamodb.AttributeValue{SS: []*string{aws.String("foo"), aws.String("bar")}}, `{"k":{"SS":["foo","bar"]}}`},
}

func TestSimpleEncoder(t *testing.T) {
	for _, test := range attrTests {
		var buf bytes.Buffer
		if err := NewSimpleEncoder(&buf).WriteItem(map[string]*dynamodb.AttributeValue{
			"k": test.src,
		}); err != nil {
			t.Errorf("Unexpected error test=%q error=%v", test.name, err)
		}
		if val := buf.String(); val != test.expected+"\n" {
			t.Errorf("test=%q expected=%s actual=%s", test.name, test.expected, val)
		}
	}
}

func TestSimpleDecoder(t *testing.T) {
	buf := strings.NewReader(`{"k":{"S":"foo"}}`)
	dec := NewSimpleDecoder(buf)
	item, err := dec.ReadItem()
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	expected := map[string]*dynamodb.AttributeValue{
		"k": &dynamodb.AttributeValue{S: aws.String("foo")},
	}
	if !reflect.DeepEqual(item, expected) {
		t.Errorf("expected=%#v actual=%#v", expected, item)
	}
}
