// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// attributeValue is a copy of dynamodb.AttributeValue with some json
// tags added to avoid encoding omitted entries when writing out the dump.
type attributeValue struct {
	// A Binary data type.
	//
	// B is automatically base64 encoded/decoded by the SDK.
	B []byte `json:",omitempty"`

	// A Boolean data type.
	BOOL *bool `json:",omitempty"`

	// A Binary Set data type.
	BS [][]byte `json:",omitempty"`

	// A List of attribute values.
	L []*attributeValue `json:",omitempty"`

	// A Map of attribute values.
	M map[string]*attributeValue `json:",omitempty"`

	// A Number data type.
	N *string `json:",omitempty"`

	// A Number Set data type.
	NS []*string `json:",omitempty"`

	// A Null data type.
	NULL *bool `json:",omitempty"`

	// A String data type.
	S *string `json:",omitempty"`

	// A String Set data type.
	SS []*string `json:",omitempty"`
}

func toAttribute(src *dynamodb.AttributeValue) (dst *attributeValue) {
	dst = &attributeValue{
		B:    src.B,
		BOOL: src.BOOL,
		BS:   src.BS,
		N:    src.N,
		NS:   src.NS,
		NULL: src.NULL,
		S:    src.S,
		SS:   src.SS,
	}
	if src.L != nil {
		dst.L = make([]*attributeValue, len(src.L))
		for i := range src.L {
			dst.L[i] = toAttribute(src.L[i])
		}
	}
	if src.M != nil {
		dst.M = make(map[string]*attributeValue)
		for k, v := range src.M {
			dst.M[k] = toAttribute(v)
		}
	}
	return dst
}

// SimpleEncoder implements the ItemWriter interface to convert DynamoDB
// items to a JSON stream.
type SimpleEncoder struct {
	jw *json.Encoder
	m  sync.Mutex
}

// NewSimpleEncoder creates an initializes a new SimpleEncoder.
func NewSimpleEncoder(w io.Writer) *SimpleEncoder {
	return &SimpleEncoder{
		jw: json.NewEncoder(w),
	}
}

// WriteItem implemnts ItemWriter.
func (e *SimpleEncoder) WriteItem(item map[string]*dynamodb.AttributeValue) error {
	newItem := make(map[string]*attributeValue, len(item))
	for k, v := range item {
		newItem[k] = toAttribute(v)
	}
	e.m.Lock()
	err := e.jw.Encode(newItem)
	e.m.Unlock()
	return err
}

// SimpleDecoder implements the ItemReader interface to convert JSON entries
// to DynamoDB attributes items.
type SimpleDecoder struct {
	jd *json.Decoder
}

// NewSimpleDecoder creates and initializes a new SimpleDeocder.
func NewSimpleDecoder(r io.Reader) *SimpleDecoder {
	return &SimpleDecoder{
		jd: json.NewDecoder(r),
	}
}

// ReadItem implements ItemReader.
func (d *SimpleDecoder) ReadItem() (item map[string]*dynamodb.AttributeValue, err error) {
	err = d.jd.Decode(&item)
	return item, err
}
