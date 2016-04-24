// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	binType    = "binary"
	binSetType = "binary-set"
	boolType   = "bool"
	listType   = "list"
	mapType    = "map"
	numType    = "number"
	numSetType = "number-set"
	nullType   = "null"
	strType    = "string"
	strSetType = "string-set"
)

type attrTypeValue struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// ItemWriter is the interface expected by a Fetcher when writing retrieved
// DynamoDB items.  Implementors must take care to support writes from
// concurrent goroutines.
type ItemWriter interface {
	WriteItem(item map[string]*dynamodb.AttributeValue) error
}

func (jie *JSONItemEncoder) encodeVal(vtype string, val interface{}) interface{} {
	if jie.encodeTypes {
		return attrTypeValue{vtype, val}
	}
	return val
}

func (jie *JSONItemEncoder) attrToValue(av *dynamodb.AttributeValue) (val interface{}) {
	switch {
	case av.B != nil: // binary
		return jie.encodeVal(binType, string(av.B))

	case av.BOOL != nil:
		return jie.encodeVal(boolType, *av.BOOL)

	case av.BS != nil: // binary set
		result := make([]string, 0, len(av.BS))
		for _, v := range av.BS {
			result = append(result, string(v))
		}
		return jie.encodeVal(binSetType, result)

	case av.L != nil: // list of attributes
		result := make([]interface{}, 0, len(av.L))
		for _, av := range av.L {
			result = append(result, jie.attrToValue(av))
		}
		return jie.encodeVal(listType, result)

	case av.M != nil: // map of attributes
		result := make(map[string]interface{})
		for k, v := range av.M {
			result[k] = jie.attrToValue(v)
		}
		return jie.encodeVal(mapType, result)

	case av.N != nil: // number
		if jie.numbersAsStrings {
			return jie.encodeVal(numType, *av.N)
		}
		n, _ := strconv.ParseFloat(*av.N, 64)
		return jie.encodeVal(numType, n)

	case av.NS != nil: // number set
		result := make([]interface{}, 0, len(av.NS))
		for _, v := range av.NS {
			if jie.numbersAsStrings {
				result = append(result, *v)
			} else {
				n, _ := strconv.ParseFloat(*v, 64)
				result = append(result, n)
			}
		}
		return jie.encodeVal(numSetType, result)

	case av.NULL != nil: // null
		return jie.encodeVal(nullType, nil)

	case av.S != nil: // string
		return jie.encodeVal(strType, *av.S)

	case av.SS != nil: // string set
		result := make([]string, 0, len(av.SS))
		for _, v := range av.SS {
			result = append(result, *v)
		}
		return jie.encodeVal(strSetType, result)
	}
	panic(fmt.Sprintf("attribute value with no handled type: %#v", av))
}

// JSONItemEncoder implements the ItemWriter interface, formatting
// incoming items as JSON and writing them to the supplied writer.
type JSONItemEncoder struct {
	m                sync.Mutex
	jw               *json.Encoder
	encodeTypes      bool
	numbersAsStrings bool
}

// NewJSONItemEncoder returns a JSONItmeEncoder writing to the supplied io.Writer.
func NewJSONItemEncoder(w io.Writer, encodeTypes, numbersAsStrings bool) *JSONItemEncoder {
	return &JSONItemEncoder{
		jw:               json.NewEncoder(w),
		encodeTypes:      encodeTypes,
		numbersAsStrings: numbersAsStrings,
	}
}

// WriteItem JSON encodes a single DynamoDB item.
func (jie *JSONItemEncoder) WriteItem(item map[string]*dynamodb.AttributeValue) error {
	jie.m.Lock()
	defer jie.m.Unlock()

	data := make(map[string]interface{})
	for k, av := range item {
		data[k] = jie.attrToValue(av)
	}
	return jie.jw.Encode(data)
}
