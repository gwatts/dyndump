// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func hashData(data string) string {
	h := sha256.New()
	h.Write([]byte(data))
	return fmt.Sprintf("%x", h.Sum(nil))
}

type dataset struct {
	keyNames    []string
	dataByKey   map[string]string
	hashesByKey map[string]string
	masterHash  string
}

func makeData(count int) dataset {
	ds := dataset{
		dataByKey:   make(map[string]string),
		hashesByKey: make(map[string]string),
	}
	h := sha256.New()
	for i := 0; i < count; i++ {
		k := fmt.Sprintf("key%d", i)
		data := fmt.Sprintf("data%d\n", i)
		ds.keyNames = append(ds.keyNames, k)
		ds.dataByKey[k] = data
		ds.hashesByKey[k] = hashData(data)
		fmt.Fprintf(h, "%s\n", hashData(data))
	}
	ds.masterHash = fmt.Sprintf("%x", h.Sum(nil))
	return ds
}

func TestS3ReadOK(t *testing.T) {
	for _, failmode := range []string{"ok", "breakmaster", "breakpiece"} {
		testdata := makeData(4)

		switch failmode {
		case "breakmaster":
			testdata.masterHash = "incorrect"
		case "breakpiece":
			testdata.hashesByKey[testdata.keyNames[1]] = "incorrect"
		}

		f := &fakeS3GetLister{
			list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
				//once.Do(func() {
				if bucketName := aws.StringValue(input.Bucket); bucketName != "test-bucket" {
					return fmt.Errorf("incorrect bucket for list %q", bucketName)
				}
				if prefix := aws.StringValue(input.Prefix); prefix != "test-prefix-part-" {
					return fmt.Errorf("incorrect prefix for list %q", prefix)
				}

				// generate a couple of pages
				for i := 0; i < 2; i++ {
					k1 := testdata.keyNames[0+(2*i)]
					k2 := testdata.keyNames[1+(2*i)]
					//k1 := fmt.Sprintf("key%d", 0+(2*i))
					//k2 := fmt.Sprintf("key%d", 1+(2*i))
					page := &s3.ListObjectsOutput{
						Contents: []*s3.Object{
							{Key: aws.String(k1)},
							{Key: aws.String(k2)},
						},
					}
					cont := fn(page, i == 1)
					if !cont {
						return nil
					}
				}
				return nil
			},

			get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
				key := aws.StringValue(input.Key)

				if bucketName := aws.StringValue(input.Bucket); bucketName != "test-bucket" {
					return nil, fmt.Errorf("incorrect bucket for get %q", bucketName)
				}
				if strings.HasSuffix(key, "-meta.json") {
					meta := Metadata{
						Hash:       testdata.masterHash,
						PartCount:  4,
						LastHashed: 4,
					}
					js, _ := json.Marshal(meta)
					resp := &s3.GetObjectOutput{
						Body: ioutil.NopCloser(bytes.NewReader(js)),
					}
					return resp, nil
				}

				fmt.Printf("Get key %q\n", aws.StringValue(input.Key))
				resp := &s3.GetObjectOutput{
					Body: ioutil.NopCloser(strings.NewReader(testdata.dataByKey[key])),
					Metadata: map[string]*string{
						http.CanonicalHeaderKey(metaSha256): aws.String(testdata.hashesByKey[key]),
					},
				}

				return resp, nil
			},
		}

		r := &S3Reader{
			S3:         f,
			Bucket:     "test-bucket",
			PathPrefix: "test-prefix",
		}

		data, err := ioutil.ReadAll(r)
		switch failmode {
		case "ok":
			if err != nil {
				t.Error("Unexpected error for failmode=ok", err)
				continue
			}

			expected := "data0\ndata1\ndata2\ndata3\n"
			if s := string(data); s != expected {
				t.Errorf("expected=%q actual=%q", expected, s)
			}

		default:
			// should fail
			if err == nil {
				t.Error("failmode=%s err=%v", failmode, err)
			}
		}
	}
}

// Check that an error response from list objects translates into a read error
func TestS3ReadListFailed(t *testing.T) {
	var testError = errors.New("test error")

	f := &fakeS3GetLister{
		list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
			return testError
		},
		get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			js, _ := json.Marshal(Metadata{})
			resp := &s3.GetObjectOutput{
				Body: ioutil.NopCloser(bytes.NewReader(js)),
			}
			return resp, nil
		},
	}

	r := &S3Reader{
		S3:         f,
		Bucket:     "test-bucket",
		PathPrefix: "test-prefix",
	}

	_, err := ioutil.ReadAll(r)
	if err != testError {
		t.Error("Incorrect error response", err)
	}
}

// Check that an error from GetObject translates into a read error
func TestS3GetFailed(t *testing.T) {
	var testError = errors.New("test error")

	f := &fakeS3GetLister{
		list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
			page := &s3.ListObjectsOutput{
				Contents: []*s3.Object{
					{Key: aws.String("key00")},
				},
			}
			fn(page, false)
			return nil
		},

		get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			return nil, testError
		},
	}

	r := &S3Reader{
		S3:         f,
		Bucket:     "test-bucket",
		PathPrefix: "test-prefix",
	}

	_, err := ioutil.ReadAll(r)
	if err != testError {
		t.Error("Incorrect error resposne", err)
	}
}

// Check that a body read failure from S3 is propagated to a read error
func TestS3ReadFailed(t *testing.T) {
	var testError = errors.New("test error")

	f := &fakeS3GetLister{
		list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
			page := &s3.ListObjectsOutput{
				Contents: []*s3.Object{
					{Key: aws.String("key00")},
				},
			}
			fn(page, false)
			return nil
		},

		get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			if strings.HasSuffix(aws.StringValue(input.Key), "-meta.json") {
				meta := Metadata{}
				js, _ := json.Marshal(meta)
				resp := &s3.GetObjectOutput{
					Body: ioutil.NopCloser(bytes.NewReader(js)),
				}
				return resp, nil
			}
			resp := &s3.GetObjectOutput{
				Body: ioutil.NopCloser(&errReader{content: strings.NewReader("test"), err: testError}),
			}
			return resp, nil
		},
	}

	r := &S3Reader{
		S3:         f,
		Bucket:     "test-bucket",
		PathPrefix: "test-prefix",
	}

	_, err := ioutil.ReadAll(r)
	if err != testError {
		t.Error("Incorrect error resposne", err)
	}
}

func TestS3ReadMetadata(t *testing.T) {
	f := &fakeS3GetLister{
		get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			if bucketName := aws.StringValue(input.Bucket); bucketName != "test-bucket" {
				return nil, fmt.Errorf("incorrect bucket for list %q", bucketName)
			}
			if k := aws.StringValue(input.Key); k != "test-prefix-meta.json" {
				return nil, errors.New("Incorrect key name " + k)
			}
			resp := &s3.GetObjectOutput{
				Body: ioutil.NopCloser(strings.NewReader(`{"table_name":"a_table","item_count":1234}`)),
			}
			return resp, nil
		},
	}

	r := &S3Reader{
		S3:         f,
		Bucket:     "test-bucket",
		PathPrefix: "test-prefix",
	}

	md, err := r.Metadata()
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	if md.TableName != "a_table" || md.ItemCount != 1234 {
		t.Error("incorrect metadata", md)
	}
}

type fakeS3GetLister struct {
	list func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error
	get  func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

func (s3 *fakeS3GetLister) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return s3.get(input)
}

func (s3 *fakeS3GetLister) ListObjectsPages(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
	return s3.list(input, fn)
}

type errReader struct {
	content io.Reader
	err     error
}

func (r *errReader) Read(p []byte) (n int, err error) {
	n, err = r.content.Read(p)
	if err != nil {
		return n, r.err
	}
	return n, err
}
