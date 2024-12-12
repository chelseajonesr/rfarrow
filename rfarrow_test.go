// Copyright 2023 Rivian Automotive, Inc.
// Licensed under the Apache License, Version 2.0 (the “License”);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an “AS IS” BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rfarrow

import (
	"bytes"
	"reflect"
	"testing"
	"unicode"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/apache/arrow/go/v14/parquet/schema"
)

func ptr[T any](v T) *T {
	return &v
}
func TestRoundTripSimpleTypes(t *testing.T) {
	type SimpleTypes struct {
		AnInt    int     `parquet:"name=anInt"`
		AnInt8   int8    `parquet:"name=anInt8"`
		AnInt16  int16   `parquet:"name=anInt16"`
		AnInt32  int32   `parquet:"name=anInt32"`
		AnInt64  int64   `parquet:"name=anInt64"`
		AFloat32 float32 `parquet:"name=aFloat32"`
		AFloat64 float64 `parquet:"name=aFloat64"`
		AString  string  `parquet:"name=aString, converted=UTF8"`
		ABool    bool    `parquet:"name=aBool"`
		AUint    uint    `parquet:"name=aUint"`
		AUint8   uint8   `parquet:"name=aUint8"`
		AUint16  uint16  `parquet:"name=aUint16"`
		AUint32  uint32  `parquet:"name=aUint32"`
		AUint64  uint64  `parquet:"name=aUint64"`
	}

	simpleTypes := []SimpleTypes{
		{AnInt: 1, AnInt8: 2, AnInt16: 3, AnInt32: -50, AnInt64: 8223372036854775807, AFloat32: 3.1415, AFloat64: -1.1234, AString: "Good morning!", ABool: true, AUint: 12, AUint8: 7, AUint16: 5, AUint32: 32, AUint64: 6543},
		{AnInt: -42, AnInt8: 12, AnInt16: -34, AnInt32: 12345, AnInt64: -100000, AFloat32: 0, AFloat64: 1, AString: "", ABool: false, AUint: 912, AUint8: 4, AUint16: 55, AUint32: 23, AUint64: 3456},
	}

	compareRoundTrip(t, simpleTypes)

	type PointerTypes struct {
		AnInt    *int     `parquet:"name=anInt"`
		AnInt8   *int8    `parquet:"name=anInt8"`
		AnInt16  *int16   `parquet:"name=anInt16"`
		AnInt32  *int32   `parquet:"name=anInt32"`
		AnInt64  *int64   `parquet:"name=anInt64"`
		AFloat32 *float32 `parquet:"name=aFloat32"`
		AFloat64 *float64 `parquet:"name=aFloat64"`
		AString  *string  `parquet:"name=aString, converted=UTF8"`
		ABool    *bool    `parquet:"name=aBool"`
		AUint    *uint    `parquet:"name=aUint"`
		AUint8   *uint8   `parquet:"name=aUint8"`
		AUint16  *uint16  `parquet:"name=aUint16"`
		AUint32  *uint32  `parquet:"name=aUint32"`
		AUint64  *uint64  `parquet:"name=aUint64"`
	}
	simpleToPointer := func(s *SimpleTypes) PointerTypes {
		return PointerTypes{
			AnInt:    &s.AnInt,
			AnInt8:   &s.AnInt8,
			AnInt16:  &s.AnInt16,
			AnInt32:  &s.AnInt32,
			AnInt64:  &s.AnInt64,
			AFloat32: &s.AFloat32,
			AFloat64: &s.AFloat64,
			AString:  &s.AString,
			ABool:    &s.ABool,
			AUint:    &s.AUint,
			AUint8:   &s.AUint8,
			AUint16:  &s.AUint16,
			AUint32:  &s.AUint32,
			AUint64:  &s.AUint64,
		}
	}

	pointerTypes := make([]PointerTypes, len(simpleTypes))
	for i, s := range simpleTypes {
		pointerTypes[i] = simpleToPointer(&s)
	}
	compareRoundTrip(t, pointerTypes)
}

// Convert the values to Parquet and back, and make sure the results are equal
func compareRoundTrip[T any](t *testing.T, values []T) {
	t.Helper()
	compareRoundTripWithAdjustments(t, values, func(i int, expected T) T { return expected })
}

// Convert the values to Parquet and back, and make sure the results are equal
// The function adjustedExpected allows modification of the expected value in situations where round trip doesn't return
// exactly the same thing (nil map -> empty map, etc)
func compareRoundTripWithAdjustments[T any](t *testing.T, values []T, adjustExpected func(i int, expected T) T) {
	t.Helper()
	buf := new(bytes.Buffer)
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	err := WriteGoStructsToParquet(values, buf, props)
	if err != nil {
		t.Fatal(err)
	}
	bytesReader := bytes.NewReader(buf.Bytes())
	results, err := ReadGoStructsFromParquet[T](bytesReader, false)
	if err != nil {
		t.Fatal(err)
	}
	for i, expected := range values {
		adjustedExpected := adjustExpected(i, expected)
		if !reflect.DeepEqual(*results[i], adjustedExpected) {
			t.Errorf("expected %v got %v", adjustedExpected, *results[i])
		}
	}
}

func TestRoundTripNestedTypes(t *testing.T) {
	type NestedTypes2 struct {
		AnInt     int32   `parquet:"name=anInt"`
		AString   string  `parquet:"name=aString, converted=UTF8"`
		APtrToPtr **int32 `parquet:"name=aPtrToPtr"`
	}
	type NestedTypes1 struct {
		AMap          map[int64]NestedTypes2 `parquet:"name=aMap"`
		ANestedTypes2 NestedTypes2           `parquet:"name=aNestedTypes2"`
	}
	type NestedTypes struct {
		AMap          map[string]string  `parquet:"name=aMap, keyconverted=UTF8, valueconverted=UTF8"`
		AList         []int32            `parquet:"name=aList"`
		AMapPtr       *map[string]string `parquet:"name=aMapPtr, keyconverted=UTF8, valueconverted=UTF8"`
		AListPtr      *[]int32           `parquet:"name=aListPtr"`
		ANestedTypes1 NestedTypes1       `parquet:"name=aNestedTypes1"`
		ANestedTypes2 *NestedTypes2      `parquet:"name=aNestedTypes2"`
		AListNested   []NestedTypes2     `parquet:"name=aListNested"`
		ASliceOfPtrs  []*int32           `parquet:"name=aSliceOfPtrs"`
	}

	someInts := []int32{
		1, 2, 3,
	}
	someIntPtrs := []*int32{
		&someInts[0], &someInts[1], &someInts[2], nil,
	}
	nestedTypes2 := []NestedTypes2{
		{AnInt: 1, AString: "one", APtrToPtr: &someIntPtrs[0]},
		{AnInt: 2, AString: "two", APtrToPtr: &someIntPtrs[1]},
		{AnInt: 3, AString: "three", APtrToPtr: &someIntPtrs[3]},
		{AnInt: -4, AString: "negative four", APtrToPtr: &someIntPtrs[2]},
		{AnInt: 5, AString: "", APtrToPtr: &someIntPtrs[0]},
	}

	nestedTypes1 := []NestedTypes1{
		{AMap: map[int64]NestedTypes2{1234: nestedTypes2[0], 567: nestedTypes2[1]}, ANestedTypes2: nestedTypes2[1]},
		{AMap: map[int64]NestedTypes2{89: nestedTypes2[3], 0: nestedTypes2[3]}, ANestedTypes2: nestedTypes2[4]},
		{AMap: map[int64]NestedTypes2{}, ANestedTypes2: nestedTypes2[0]},
		{AMap: nil, ANestedTypes2: nestedTypes2[2]},
	}

	nestedTypes := []NestedTypes{
		{
			AMap: map[string]string{
				"hello": "bye",
				"blue":  "green",
				"empty": "",
			},
			AList:         []int32{1, 2, 3, 4},
			AMapPtr:       ptr(map[string]string{"ok": "okay", "pencil": "mechanical"}),
			AListPtr:      nil,
			ANestedTypes1: nestedTypes1[2],
			ANestedTypes2: nil,
			AListNested:   []NestedTypes2{nestedTypes2[0], nestedTypes2[3]},
			ASliceOfPtrs:  []*int32{},
		},
		{
			AMap: map[string]string{
				"apple":  "banana",
				"coffee": "tea",
			},
			AList:         []int32{1, 1, 1},
			AMapPtr:       nil,
			AListPtr:      ptr([]int32{987, 654}),
			ANestedTypes1: nestedTypes1[1],
			ANestedTypes2: ptr(nestedTypes2[0]),
			AListNested:   []NestedTypes2{},
			ASliceOfPtrs:  []*int32{},
		},
		{
			AMap:          map[string]string{},
			AList:         []int32{},
			ANestedTypes1: nestedTypes1[2],
			AListNested:   []NestedTypes2{nestedTypes2[0]},
			ASliceOfPtrs:  someIntPtrs,
		},
		{
			AMap:          nil,
			AList:         nil,
			ANestedTypes1: nestedTypes1[0],
			AListNested:   nil,
			ASliceOfPtrs:  []*int32{nil, nil},
		},
		{
			AMap:          nil,
			AList:         nil,
			ANestedTypes1: nestedTypes1[3],
			AListNested:   nil,
			ASliceOfPtrs:  []*int32{},
		},
	}
	expectedWithoutNilMapsSlices := 3
	compareRoundTripWithAdjustments(t, nestedTypes, func(i int, expected NestedTypes) NestedTypes {
		if i >= expectedWithoutNilMapsSlices {
			// Arrow will read nil maps/slices as empty maps/slices, so adjust our expected values
			if expected.AList == nil {
				expected.AList = make([]int32, 0)
			}
			if expected.AMap == nil {
				expected.AMap = make(map[string]string)
			}
			if expected.AListNested == nil {
				expected.AListNested = make([]NestedTypes2, 0)
			}
			if expected.ANestedTypes1.AMap == nil {
				expected.ANestedTypes1.AMap = make(map[int64]NestedTypes2)
			}
			if expected.ANestedTypes1.ANestedTypes2.APtrToPtr != nil && *expected.ANestedTypes1.ANestedTypes2.APtrToPtr == nil {
				expected.ANestedTypes1.ANestedTypes2.APtrToPtr = nil
			}
		}
		return expected
	})
}

func TestRoundTripConvertedTypes(t *testing.T) {
	type ConvertedTypes struct {
		Utf8           string `parquet:"name=utf8, converted=UTF8"`
		Uint32         uint32 `parquet:"name=toInt, converted=INT_32"`
		Int64          int64  `parquet:"name=toUInt, converted=UINT_64"`
		DateInt        int32  `parquet:"name=dateInt, converted=date"`
		TimeMilli      int32  `parquet:"name=timemilli, converted=TIME_MILLIS"`
		TimeMicro      int64  `parquet:"name=timemicro, converted=time_micros"`
		TimeStampMilli int64  `parquet:"converted=timestamp_millis"`
		TimeStampMicro int64  `parquet:"converted=timestamp_micros"`
		Fixed          []byte `parquet:"type=fixed_len_byte_array, length=12"`
	}

	convertedTypes := []ConvertedTypes{
		{
			Utf8:           "hello",
			Uint32:         1234,
			Int64:          4321,
			DateInt:        20,
			TimeMilli:      101,
			TimeMicro:      12345678,
			TimeStampMilli: 222222,
			TimeStampMicro: 333333,
			Fixed:          []byte{1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	compareRoundTrip(t, convertedTypes)
}

func TestMapGoStructFieldNamesToArrowIndices(t *testing.T) {
	type SimpleTypes struct {
		AnInt   int   `parquet:"name=anInt"`
		AnInt8  int8  `parquet:"name=anInt8"`
		AnInt16 int16 `parquet:"name=anInt16"`
		AnInt32 int32 `parquet:"name=anInt32"`
		AnInt64 int64 `parquet:"name=anInt64"`
	}

	parquetSchema, err := schema.NewSchemaFromStruct(new(SimpleTypes))
	if err != nil {
		t.Fatal(err)
	}
	arrowSchema, err := pqarrow.FromParquet(parquetSchema, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	arrowFields := arrowSchema.Fields()

	// Get mappings between struct member names and parquet/arrow names so we don't have to look them up repeatedly
	// during record assignments
	structFieldNameToArrowIndexMappings, err := MapGoStructFieldNamesToArrowIndices[SimpleTypes](arrowFields, []string{}, false, false)
	if err != nil {
		t.Fatal(err)
	}

	// Expect to see all fields, in the same order
	if len(structFieldNameToArrowIndexMappings) != 5 {
		t.Errorf("expected %d fields in mapping, found %d", 5, len(structFieldNameToArrowIndexMappings))
	}
	for n, i := range structFieldNameToArrowIndexMappings {
		r := []rune(arrowFields[i].Name)
		s := string(append([]rune{unicode.ToUpper(r[0])}, r[1:]...))
		if s != n {
			t.Errorf("expected field %d = %s, found %s", i, n, s)
		}
	}

	// Nested types
	type ChildType struct {
		AnInt  int  `parquet:"name=anInt"`
		AnInt8 int8 `parquet:"name=anInt8"`
	}

	type NestedTypes struct {
		AnInt32    int32     `parquet:"name=anInt32"`
		AnInt64    int64     `parquet:"name=anInt64"`
		AChildType ChildType `parquet:"name=aChildType"`
	}

	parquetSchema, err = schema.NewSchemaFromStruct(new(NestedTypes))
	if err != nil {
		t.Fatal(err)
	}
	arrowSchema, err = pqarrow.FromParquet(parquetSchema, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	arrowFields = arrowSchema.Fields()

	structFieldNameToArrowIndexMappings, err = MapGoStructFieldNamesToArrowIndices[NestedTypes](arrowFields, []string{}, false, false)
	if err != nil {
		t.Fatal(err)
	}

	if len(structFieldNameToArrowIndexMappings) != 5 {
		t.Errorf("expected %d fields in mapping, found %d", 5, len(structFieldNameToArrowIndexMappings))
	}
	expectedMap := map[string]int{
		"AnInt32":           0,
		"AnInt64":           1,
		"AChildType":        2,
		"AChildType.AnInt":  0,
		"AChildType.AnInt8": 1,
	}
	if !reflect.DeepEqual(expectedMap, structFieldNameToArrowIndexMappings) {
		t.Errorf("expected %v, got %v", expectedMap, structFieldNameToArrowIndexMappings)
	}

	// Skip some fields, leaving indices as is
	structFieldNameToArrowIndexMappings, err = MapGoStructFieldNamesToArrowIndices[NestedTypes](arrowFields, []string{"AnInt64", "AChildType.AnInt"}, false, false)
	if err != nil {
		t.Fatal(err)
	}

	if len(structFieldNameToArrowIndexMappings) != 3 {
		t.Errorf("expected %d fields in mapping, found %d", 3, len(structFieldNameToArrowIndexMappings))
	}
	expectedMap = map[string]int{
		"AnInt32":           0,
		"AChildType":        2,
		"AChildType.AnInt8": 1,
	}
	if !reflect.DeepEqual(expectedMap, structFieldNameToArrowIndexMappings) {
		t.Errorf("expected %v, got %v", expectedMap, structFieldNameToArrowIndexMappings)
	}

	// Skip some fields, also skipping indices
	structFieldNameToArrowIndexMappings, err = MapGoStructFieldNamesToArrowIndices[NestedTypes](arrowFields, []string{"AnInt64", "AChildType.AnInt"}, true, false)
	if err != nil {
		t.Fatal(err)
	}

	if len(structFieldNameToArrowIndexMappings) != 3 {
		t.Errorf("expected %d fields in mapping, found %d", 3, len(structFieldNameToArrowIndexMappings))
	}
	expectedMap = map[string]int{
		"AnInt32":           0,
		"AChildType":        1,
		"AChildType.AnInt8": 0,
	}
	if !reflect.DeepEqual(expectedMap, structFieldNameToArrowIndexMappings) {
		t.Errorf("expected %v, got %v", expectedMap, structFieldNameToArrowIndexMappings)
	}

	// Extra fields in the Go struct that won't be in the arrow schema
	type NestedTypesExtraGoFields struct {
		ABool      bool      `parquet:"name=aBool"` // extra field
		AnInt32    int32     `parquet:"name=anInt32"`
		AnInt32v2  int32     `parquet:"name=anInt32v2"` // extra field
		AnInt64    int64     `parquet:"name=anInt64"`
		AChildType ChildType `parquet:"name=aChildType"`
		AnInt32v3  int32     `parquet:"name=anInt32v3"` // extra field
	}

	// Trying to read a struct with extra fields should fail when skipFieldsNotFound is false
	_, err = MapGoStructFieldNamesToArrowIndices[NestedTypesExtraGoFields](arrowFields, []string{}, false, false)
	if err == nil {
		t.Error("expected error, got nil")
	}

	// Trying to read a struct with extra fields should succeed when skipFieldsNotFound is true
	structFieldNameToArrowIndexMappings, err = MapGoStructFieldNamesToArrowIndices[NestedTypesExtraGoFields](arrowFields, []string{}, false, true)
	if err != nil {
		t.Fatal(err)
	}

	expectedMap = map[string]int{
		"AnInt32":           0,
		"AnInt64":           1,
		"AChildType":        2,
		"AChildType.AnInt":  0,
		"AChildType.AnInt8": 1,
	}

	if !reflect.DeepEqual(expectedMap, structFieldNameToArrowIndexMappings) {
		t.Errorf("expected %v, got %v", expectedMap, structFieldNameToArrowIndexMappings)
	}
}

func TestSerializeDeserializeDifferentType(t *testing.T) {
	type ChildType1 struct {
		Count     int  `parquet:"name=count"`
		Direction int8 `parquet:"name=direction"`
	}

	type NestedTypes1 struct {
		ID     int32      `parquet:"name=id"`
		Value  int64      `parquet:"name=value"`
		Detail ChildType1 `parquet:"name=detail"`
		Ext    string     `parquet:"name=ext"`
	}

	type ChildType2 struct {
		Direction int `parquet:"name=direction"`
		Counter   int `parquet:"name=count"`
	}

	type NestedTypes2 struct {
		AChildType2 ChildType2 `parquet:"name=detail"`
		ID          int        `parquet:"name=id"`
		Recorded    int32      `parquet:"name=value"`
		Ext         []byte     `parquet:"name=ext"`
	}

	type NestedTypes2WithExtraFields struct {
		Extra1      int        `parquet:"name=extra1"`
		AChildType2 ChildType2 `parquet:"name=detail"`
		ID          int        `parquet:"name=id"`
		Recorded    int32      `parquet:"name=value"`
		Extra2      int        `parquet:"name=extra2"`
		Ext         []byte     `parquet:"name=ext"`
	}

	original := []NestedTypes1{
		{ID: 1, Value: 100, Detail: ChildType1{Count: 50, Direction: 1}, Ext: "hello"},
		{ID: 2, Value: 101, Detail: ChildType1{Count: 25, Direction: 0}, Ext: ""},
		{ID: 3, Value: 1, Detail: ChildType1{Count: 17, Direction: 1}, Ext: "3!! Or maybe 2?"},
	}
	buf := new(bytes.Buffer)
	err := WriteGoStructsToParquet(original, buf, nil)
	if err != nil {
		t.Fatal(err)
	}

	deserialized, err := ReadGoStructsFromParquet[NestedTypes2](bytes.NewReader(buf.Bytes()), false)
	if err != nil {
		t.Fatal(err)
	}

	expected := []*NestedTypes2{
		{ID: 1, Recorded: 100, AChildType2: ChildType2{Counter: 50, Direction: 1}, Ext: []byte("hello")},
		{ID: 2, Recorded: 101, AChildType2: ChildType2{Counter: 25, Direction: 0}, Ext: []byte{}},
		{ID: 3, Recorded: 1, AChildType2: ChildType2{Counter: 17, Direction: 1}, Ext: []byte("3!! Or maybe 2?")},
	}

	if !reflect.DeepEqual(deserialized, expected) {
		t.Errorf("expected %v, found %v", expected, deserialized)
	}

	_, err = ReadGoStructsFromParquet[NestedTypes2WithExtraFields](bytes.NewReader(buf.Bytes()), false)
	if err == nil {
		t.Error("expected error, got nil")
	}

	deserializedWithExtraFields, err := ReadGoStructsFromParquet[NestedTypes2WithExtraFields](bytes.NewReader(buf.Bytes()), true)
	if err != nil {
		t.Fatal(err)
	}
	expectedWithExtraFields := []*NestedTypes2WithExtraFields{
		{Extra1: 0, ID: 1, Recorded: 100, AChildType2: ChildType2{Counter: 50, Direction: 1}, Extra2: 0, Ext: []byte("hello")},
		{Extra1: 0, ID: 2, Recorded: 101, AChildType2: ChildType2{Counter: 25, Direction: 0}, Extra2: 0, Ext: []byte{}},
		{Extra1: 0, ID: 3, Recorded: 1, AChildType2: ChildType2{Counter: 17, Direction: 1}, Extra2: 0, Ext: []byte("3!! Or maybe 2?")},
	}

	if !reflect.DeepEqual(deserializedWithExtraFields, expectedWithExtraFields) {
		t.Errorf("expected %v, found %v", expectedWithExtraFields, deserializedWithExtraFields)
	}
}

// TODO test multiple records inside table in ReadGoStructsFromParquet
