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
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/apache/arrow/go/v14/arrow/float16"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/apache/arrow/go/v14/parquet/schema"
)

var (
	ErrorArrowConversion error = errors.New("error converting from arrow")
	ErrorNotImplemented  error = errors.New("not implemented")
)

// / Convert Parquet to an array of Go structs of type T
func ReadGoStructsFromParquet[T any](reader parquet.ReaderAtSeeker) ([]*T, error) {
	parquetReader, err := file.NewParquetReader(reader)
	if err != nil {
		return nil, err
	}

	parquetSchema := parquetReader.MetaData().Schema
	arrowSchema, err := pqarrow.FromParquet(parquetSchema, nil, nil)
	if err != nil {
		return nil, err
	}
	arrowFields := arrowSchema.Fields()

	// Get mappings between struct member names and parquet/arrow names so we don't have to look them up repeatedly
	// during record assignments
	structFieldNameToArrowIndexMappings, err := MapGoStructFieldNamesToArrowIndices[T](arrowFields, []string{}, false, true)
	if err != nil {
		return nil, err
	}

	fileReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 1, Parallel: false}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}

	tbl, err := fileReader.ReadTable(context.TODO())
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	tableReader := array.NewTableReader(tbl, 0)
	defer tableReader.Release()

	entries := make([]*T, tbl.NumRows())
	entryValues := make([]reflect.Value, tbl.NumRows())
	recordRowOffset := int64(0)

	for tableReader.Next() {
		record := tableReader.Record()

		// Pre-allocate the go structs
		for j := int64(0); j < record.NumRows(); j++ {
			t := new(T)
			entries[j+recordRowOffset] = t
			entryValues[j+recordRowOffset] = reflect.ValueOf(t)
		}

		err = SetGoStructsFromArrowArrays(entryValues[recordRowOffset:recordRowOffset+record.NumRows()], record.Columns(), structFieldNameToArrowIndexMappings, 0)
		if err != nil {
			return nil, err
		}

		recordRowOffset += record.NumRows()
	}
	return entries, nil
}

// / Given an array of go structs (as reflect values), and an array of corresponding arrow Arrays, set the go struct values recursively
// / The struct field names do not need to be in the same order as the arrow arrays, because structFieldNameToArrowIndexMappings is a list
// / of go struct fields to arrow indices, which can be generated using MapGoStructFieldNamesToArrowIndices()
// / The arrowRowOffset allows starting at an offset into the arrowArrays
func SetGoStructsFromArrowArrays(goStructs []reflect.Value, arrowArrays []arrow.Array, structFieldNameToArrowIndexMappings map[string]int, arrowRowOffset int) error {
	goType := goStructs[0].Type()
	for goType.Kind() == reflect.Pointer {
		goType = goType.Elem()
	}
	if goType.Kind() != reflect.Struct {
		return errors.Join(ErrorArrowConversion, fmt.Errorf("expected struct type but found %v", goType.Name()))
	}

	for idx, goStruct := range goStructs {
		arrowRow := idx + arrowRowOffset
		for goStruct.Kind() == reflect.Pointer {
			goStruct = goStruct.Elem()
		}

		for i := 0; i < goType.NumField(); i++ {
			goFieldName := goType.Field(i).Name
			arrowIndex, ok := structFieldNameToArrowIndexMappings[goFieldName]
			if ok {
				arrowField := arrowArrays[arrowIndex]
				if arrowField.IsNull(arrowRow) {
					continue
				}
				goField := goStruct.FieldByName(goFieldName)
				elem := traverseAndInstantiateIndirections(goField)
				err := goValueFromArrowArray(elem, arrowField, arrowRow, goFieldName, structFieldNameToArrowIndexMappings)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Given a Go reflect value, set it from the given arrow.Array (where the row is specified by arrayOffset)
// This function is recursive for nested types
// goNamePrefix is a .-delimited string of the field names to reach the current value from the original root
// goNameArrowIndexMap contains a mapping of Go field names to arrow indices, so that we don't have to look them up for each row
func goValueFromArrowArray(goValue reflect.Value, arrowArray arrow.Array, arrayOffset int, goNamePrefix string, goNameArrowIndexMap map[string]int) error {
	goType := goValue.Type()
	for goType.Kind() == reflect.Pointer {
		goType = goType.Elem()
	}

	if arrowArray.IsNull(arrayOffset) {
		return nil
	}

	elem := traverseAndInstantiateIndirections(goValue)

	switch goType.Kind() {
	case reflect.Bool:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Boolean:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetBool(arrowValue)
			return nil
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to bool", typedArrowArray.DataType().Name()))
		}
	case reflect.Float32, reflect.Float64:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Float16:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetFloat(float64(arrowValue.Float32()))
			return nil
		case *array.Float32:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetFloat(float64(arrowValue))
			return nil
		case *array.Float64:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetFloat(arrowValue)
			return nil
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to float", typedArrowArray.DataType().Name()))
		}
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Int16:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Int32:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Int64:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(arrowValue)
			return nil
		case *array.Int8:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Uint16:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Uint32:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Uint64:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Uint8:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Date32:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Time32:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Time64:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Timestamp:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Decimal128, *array.Decimal256:
			return errors.Join(ErrorNotImplemented, ErrorArrowConversion, fmt.Errorf("unable to convert %s to int", typedArrowArray.DataType().Name()))
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to int", typedArrowArray.DataType().Name()))
		}
	case reflect.String:
		switch typedArrowArray := arrowArray.(type) {
		case *array.String:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetString(arrowValue)
			return nil
		case *array.Binary:
			arrowValue := typedArrowArray.ValueString(arrayOffset)
			elem.SetString(arrowValue)
			return nil
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to string", typedArrowArray.DataType().Name()))
		}
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Uint16:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetUint(uint64(arrowValue))
			return nil
		case *array.Uint32:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetUint(uint64(arrowValue))
			return nil
		case *array.Uint64:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetUint(arrowValue)
			return nil
		case *array.Uint8:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetUint(uint64(arrowValue))
			return nil
		case *array.Int16:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetUint(uint64(arrowValue))
			return nil
		case *array.Int32:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetUint(uint64(arrowValue))
			return nil
		case *array.Int64:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetUint(uint64(arrowValue))
			return nil
		case *array.Int8:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem.SetUint(uint64(arrowValue))
			return nil
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to uint", typedArrowArray.DataType().Name()))
		}
	case reflect.Array, reflect.Slice:
		var start, end int64
		var values arrow.Array
		switch typedArrowArray := arrowArray.(type) {
		case *array.List:
			start, end = typedArrowArray.ValueOffsets(arrayOffset)
			values = typedArrowArray.ListValues()
		case *array.FixedSizeList:
			start, end = typedArrowArray.ValueOffsets(arrayOffset)
			values = typedArrowArray.ListValues()
		case *array.FixedSizeBinary:
			bytes := typedArrowArray.Value(arrayOffset)
			elem.SetBytes(bytes)
		case *array.Binary:
			bytes := typedArrowArray.Value(arrayOffset)
			elem.SetBytes(bytes)
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to array/slice", typedArrowArray.DataType().Name()))
		}
		for i := 0; i < int(end-start); i++ {
			entry := newReflectValueByType(goType.Elem())
			goValueFromArrowArray(entry, values, i+int(start), goNamePrefix, goNameArrowIndexMap)
			elem.Set(reflect.Append(elem, entry.Elem()))
		}
	case reflect.Map:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Map:
			keys := typedArrowArray.Keys()
			items := typedArrowArray.Items()
			var start, end int64
			start, end = typedArrowArray.ValueOffsets(arrayOffset)
			for i := 0; i < int(end-start); i++ {
				entry := newReflectValueByType(goType.Elem())
				goValueFromArrowArray(entry, items, i+int(start), goNamePrefix, goNameArrowIndexMap)
				key := newReflectValueByType(goType.Key())
				goValueFromArrowArray(key, keys, i+int(start), goNamePrefix, goNameArrowIndexMap)
				elem.SetMapIndex(key.Elem(), entry.Elem())
			}
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to map", typedArrowArray.DataType().Name()))
		}
	case reflect.Struct:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Struct:
			for i := 0; i < goType.NumField(); i++ {
				goFieldName := nestedGoFieldName(goNamePrefix, goType.Field(i).Name)
				arrowIndex, ok := goNameArrowIndexMap[goFieldName]
				if ok {
					arrowField := typedArrowArray.Field(arrowIndex)
					goField := elem.FieldByName(goType.Field(i).Name)
					err := goValueFromArrowArray(goField, arrowField, arrayOffset, goFieldName, goNameArrowIndexMap)
					if err != nil {
						return err
					}
				}
			}
		}
	default:
		return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert reflect type %s", goType.Kind().String()))
	}

	return nil
}

func nestedGoFieldName(prefix string, fieldName string) string {
	separator := ""
	if len(prefix) > 0 {
		separator = "."
	}
	return prefix + separator + fieldName
}

// Create a new value based on the given type
func newReflectValueByType(elemType reflect.Type) reflect.Value {
	var entry reflect.Value
	switch elemType.Kind() {
	case reflect.Map:
		entry = reflect.MakeMap(elemType)
	case reflect.Slice:
		entry = reflect.MakeSlice(elemType, 0, 10)
	case reflect.Pointer:
		entry = reflect.New(elemType)
	default:
		entry = reflect.New(elemType)
	}
	return entry
}

// If the given reflect value is a pointer, follow all indirections until reaching the non-pointer
// value and return that.
// Also assign default values to any nil pointer values along the way.
func traverseAndInstantiateIndirections(goValue reflect.Value) reflect.Value {
	elem := goValue
	for elem.Type().Kind() == reflect.Pointer {
		if !elem.CanSet() {
			elem = elem.Elem()
			continue
		}
		elemType := elem.Type().Elem()
		switch elemType.Kind() {
		case reflect.Map:
			newMap := reflect.MakeMap(elemType)
			newMapPtr := reflect.New(elemType)
			newMapPtr.Elem().Set(newMap)
			elem.Set(newMapPtr)
		case reflect.Slice:
			newSlice := reflect.MakeSlice(elemType, 0, 10)
			newSlicePtr := reflect.New(elemType)
			newSlicePtr.Elem().Set(newSlice)
			elem.Set(newSlicePtr)
		default:
			newElem := newReflectValueByType(elemType)
			elem.Set(newElem)
		}
		if !elem.Elem().IsValid() {
			break
		}
		elem = elem.Elem()
	}
	if elem.Type().Kind() == reflect.Map && elem.IsNil() {
		newMap := newReflectValueByType(elem.Type())
		elem.Set(newMap)
	}
	if elem.Type().Kind() == reflect.Slice && elem.IsNil() {
		newSlice := newReflectValueByType(elem.Type())
		elem.Set(newSlice)
	}
	if elem.Type().Kind() == reflect.Pointer && elem.Elem().IsValid() {
		elem = elem.Elem()
	}
	return elem
}

// / Write a slice of Go structs as Parquet
func WriteGoStructsToParquet[T any](values []T, writer io.Writer, props *parquet.WriterProperties) error {
	tbl, err := NewArrowTableFromGoStructs(values)
	if err != nil {
		return err
	}
	defer tbl.Release()

	return pqarrow.WriteTable(tbl, writer, tbl.NumRows(), props, pqarrow.DefaultWriterProps())
}

// / Convert a slice of Go structs to an Arrow table
// / The returned table needs to be released
func NewArrowTableFromGoStructs[T any](values []T) (arrow.Table, error) {
	structBuilder, arrowSchema, err := NewStructBuilderFromGoStructs(values)
	if err != nil {
		return nil, err
	}
	defer structBuilder.Release()

	arrowFieldList := arrowSchema.Fields()
	cols := make([]arrow.Column, 0, len(arrowFieldList))
	for idx, field := range arrowFieldList {
		arr := structBuilder.FieldBuilder(idx).NewArray()
		defer arr.Release()
		chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
		defer chunked.Release()
		col := arrow.NewColumn(field, chunked)
		defer col.Release()
		cols = append(cols, *col)
	}
	tbl := array.NewTable(arrowSchema, cols, int64(len(values)))
	return tbl, nil
}

// / Return a StructBuilder with all nested values filled in from the slice of Go structs
// / Also return the arrow schema in use
// / The StructBuilder needs to be released
func NewStructBuilderFromGoStructs[T any](values []T) (*array.StructBuilder, *arrow.Schema, error) {
	return NewStructBuilderFromStructsWithAdditionalNullRows(values, 0, 0)
}

// / Return a StructBuilder where there are prependNullCount empty rows at the beginning, then the slice of Go struct values are filled in,
// / then appendNullCount empty rows at the end.  There will be prependNullCount + len(values) + appendNullCount rows.
// / Also return the arrow schema in use
// / The StructBuilder needs to be released
func NewStructBuilderFromStructsWithAdditionalNullRows[T any](values []T, prependNullCount int, appendNullCount int) (*array.StructBuilder, *arrow.Schema, error) {
	defaultValue := new(T)
	parquetSchema, err := schema.NewSchemaFromStruct(defaultValue)
	if err != nil {
		return nil, nil, errors.Join(errors.New("unable to get parquet schema from struct"), err)
	}
	arrowSchema, err := pqarrow.FromParquet(parquetSchema, nil, nil)
	if err != nil {
		return nil, nil, errors.Join(errors.New("unable to get arrow schema from struct"), err)
	}
	arrowFields := arrowSchema.Fields()

	// Get mappings between struct member names and parquet/arrow names so we don't have to look them up repeatedly
	// during record assignments
	structFieldNameToArrowIndexMappings, err := MapGoStructFieldNamesToArrowIndices[T](arrowFields, []string{}, false, true)
	if err != nil {
		return nil, nil, err
	}

	structBuilder := array.NewStructBuilder(memory.DefaultAllocator, arrow.StructOf(arrowFields...))
	structBuilder.Resize(len(values) + prependNullCount + appendNullCount)
	structBuilder.AppendNulls(prependNullCount)
	for _, record := range values {
		err = AppendGoValueToArrowBuilder(reflect.ValueOf(record), structBuilder, "", structFieldNameToArrowIndexMappings)
		if err != nil {
			return nil, nil, err
		}
	}
	structBuilder.AppendNulls(appendNullCount)

	return structBuilder, arrowSchema, nil
}

// / Append a Go value to an Arrow builder
// / structFieldNameToArrowIndexMappings is a list of go struct fields to arrow indices,
// / which can be generated using MapGoStructFieldNamesToArrowIndices()
// / Recurses on nested types
func AppendGoValueToArrowBuilder(v reflect.Value, builder array.Builder, goNamePrefix string, structFieldNameToArrowIndexMappings map[string]int) error {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			builder.AppendNull()
			return nil
		}
		v = v.Elem()
	}

	if !v.IsValid() {
		builder.AppendNull()
		return nil
	}

	if (v.Kind() == reflect.Map || v.Kind() == reflect.Slice) && v.IsNil() {
		builder.AppendNull()
		return nil
	}

	switch builder.Type().ID() {
	// Non-nested types
	case arrow.INT8:
		i, err := goValueInt64(v)
		if err != nil {
			return err
		}
		builder.(*array.Int8Builder).Append(int8(i))
	case arrow.INT16:
		i, err := goValueInt64(v)
		if err != nil {
			return err
		}
		builder.(*array.Int16Builder).Append(int16(i))
	case arrow.INT32:
		i, err := goValueInt64(v)
		if err != nil {
			return err
		}
		builder.(*array.Int32Builder).Append(int32(i))
	case arrow.INT64:
		i, err := goValueInt64(v)
		if err != nil {
			return err
		}
		builder.(*array.Int64Builder).Append(i)
	case arrow.UINT8:
		i, err := goValueUint64(v)
		if err != nil {
			return err
		}
		builder.(*array.Uint8Builder).Append(uint8(i))
	case arrow.UINT16:
		i, err := goValueUint64(v)
		if err != nil {
			return err
		}
		builder.(*array.Uint16Builder).Append(uint16(i))
	case arrow.UINT32:
		i, err := goValueUint64(v)
		if err != nil {
			return err
		}
		builder.(*array.Uint32Builder).Append(uint32(i))
	case arrow.UINT64:
		i, err := goValueUint64(v)
		if err != nil {
			return err
		}
		builder.(*array.Uint64Builder).Append(i)
	case arrow.BINARY:
		switch v.Kind() {
		case reflect.String:
			builder.(*array.BinaryBuilder).Append([]byte(v.String()))
		default:
			builder.(*array.BinaryBuilder).Append(v.Bytes())
		}
	case arrow.BOOL:
		builder.(*array.BooleanBuilder).Append(v.Bool())
	case arrow.DATE32:
		builder.(*array.Date32Builder).Append(arrow.Date32(v.Int()))
	case arrow.DATE64:
		builder.(*array.Date64Builder).Append(arrow.Date64(v.Int()))
	case arrow.DECIMAL:
		switch v.Kind() {
		case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
			builder.(*array.Decimal128Builder).Append(decimal128.FromI64(v.Int()))
		case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
			builder.(*array.Decimal128Builder).Append(decimal128.FromU64(v.Uint()))
		default:
			return errors.Join(ErrorNotImplemented, fmt.Errorf("unsupported conversion from %s to arrow.DECIMAL", v.Kind()))
		}
	case arrow.DURATION:
		builder.(*array.DurationBuilder).Append(arrow.Duration(v.Int()))
	case arrow.FIXED_SIZE_BINARY:
		builder.(*array.FixedSizeBinaryBuilder).Append(v.Bytes())
	case arrow.FLOAT16:
		builder.(*array.Float16Builder).Append(float16.New(float32(v.Float())))
	case arrow.FLOAT32:
		builder.(*array.Float32Builder).Append(float32(v.Float()))
	case arrow.FLOAT64:
		builder.(*array.Float64Builder).Append(v.Float())
	case arrow.INTERVAL_DAY_TIME:
		return errors.Join(ErrorNotImplemented, errors.New("unsupported arrow type INTERVAL_DAY_TIME"))
	case arrow.INTERVAL_MONTHS:
		return errors.Join(ErrorNotImplemented, errors.New("unsupported arrow type INTERVAL_MONTHS"))
	case arrow.INTERVAL_MONTH_DAY_NANO:
		return errors.Join(ErrorNotImplemented, errors.New("unsupported arrow type INTERVAL_MONTH_DAY_NANO"))
	case arrow.NULL:
		builder.AppendNull()
	case arrow.RUN_END_ENCODED:
		return errors.Join(ErrorNotImplemented, errors.New("unsupported arrow type RUN_END_ENCODED"))
	case arrow.STRING:
		builder.(*array.StringBuilder).Append(v.String())
	case arrow.TIME32:
		builder.(*array.Time32Builder).Append(arrow.Time32(v.Int()))
	case arrow.TIME64:
		builder.(*array.Time64Builder).Append(arrow.Time64(v.Int()))
	case arrow.TIMESTAMP:
		builder.(*array.TimestampBuilder).Append(arrow.Timestamp(v.Int()))
	// Nested types
	case arrow.DICTIONARY:
		return errors.Join(ErrorNotImplemented, errors.New("unsupported arrow type DICTIONARY"))
	case arrow.FIXED_SIZE_LIST:
		listBuilder := builder.(*array.FixedSizeListBuilder)
		if v.Len() == 0 {
			listBuilder.AppendEmptyValue()
			break
		}
		listBuilder.ValueBuilder().Reserve(v.Len())
		for i := 0; i < v.Len(); i++ {
			listBuilder.Append(true)
			err := AppendGoValueToArrowBuilder(v.Index(i), listBuilder.ValueBuilder(), goNamePrefix, structFieldNameToArrowIndexMappings)
			if err != nil {
				return err
			}
		}
	case arrow.LIST:
		listBuilder := builder.(*array.ListBuilder)
		if v.Len() == 0 {
			listBuilder.AppendEmptyValue()
			break
		}
		listBuilder.ValueBuilder().Reserve(v.Len())
		listBuilder.Append(true)
		for i := 0; i < v.Len(); i++ {
			err := AppendGoValueToArrowBuilder(v.Index(i), listBuilder.ValueBuilder(), goNamePrefix, structFieldNameToArrowIndexMappings)
			if err != nil {
				return err
			}
		}
	case arrow.MAP:
		mapBuilder := builder.(*array.MapBuilder)
		if v.Len() == 0 {
			mapBuilder.AppendEmptyValue()
			break
		}
		mapBuilder.KeyBuilder().Reserve(v.Len())
		mapBuilder.ItemBuilder().Reserve(v.Len())
		mapBuilder.Append(true)
		for _, key := range v.MapKeys() {
			err := AppendGoValueToArrowBuilder(key, mapBuilder.KeyBuilder(), "", structFieldNameToArrowIndexMappings)
			if err != nil {
				return err
			}
			err = AppendGoValueToArrowBuilder(v.MapIndex(key), mapBuilder.ItemBuilder(), goNamePrefix, structFieldNameToArrowIndexMappings)
			if err != nil {
				return err
			}
		}

	case arrow.STRUCT:
		structBuilder := builder.(*array.StructBuilder)
		structBuilder.Append(true)
		for i := 0; i < v.NumField(); i++ {
			fieldName := nestedGoFieldName(goNamePrefix, v.Type().Field(i).Name)
			arrowIndex, ok := structFieldNameToArrowIndexMappings[fieldName]
			if ok {
				err := AppendGoValueToArrowBuilder(v.Field(i), structBuilder.FieldBuilder(arrowIndex), fieldName, structFieldNameToArrowIndexMappings)
				if err != nil {
					return err
				}
			}
			// If not in the goNameArrowIndexMap, we don't want to try to append it
		}
	default:
		return errors.Join(ErrorNotImplemented, fmt.Errorf("unsupported arrow type %s", builder.Type().Name()))
	}
	return nil
}

// Since reflect.Value.Int doesn't accept any uint, use this to allow converting a uint in a struct to an int in Parquet
func goValueInt64(v reflect.Value) (int64, error) {
	if v.CanInt() {
		return v.Int(), nil
	}
	if v.CanUint() {
		return int64(v.Uint()), nil
	}
	return 0, fmt.Errorf("unsupported conversion: %s to an integer type", v.Kind())
}

// And vice versa
func goValueUint64(v reflect.Value) (uint64, error) {
	if v.CanUint() {
		return v.Uint(), nil
	}
	if v.CanInt() {
		return uint64(v.Int()), nil
	}
	return 0, fmt.Errorf("unsupported conversion: %s to an unsigned integer type", v.Kind())
}

// Map struct field names to the corresponding arrow index
// Returns a map of field names to indices
// Any fields whose names match entries in goNamesToExclude will be left out, and if indicesIgnoreExcludedFields is set to true,
// then the indices will ignore the excluded fields.
func MapGoStructFieldNamesToArrowIndices[T any](arrowFields []arrow.Field, goNamesToExclude []string, indicesIgnoreExcludedFields bool, skipFieldsNotFound bool) (map[string]int, error) {
	var structValue [0]T
	structType := reflect.TypeOf(structValue).Elem()
	structFieldNameToArrowIndexMappings := make(map[string]int, len(arrowFields)*2)
	err := mapGoStructFieldNamesToArrowIndices(structType, "", arrowFields, goNamesToExclude, indicesIgnoreExcludedFields, structFieldNameToArrowIndexMappings, skipFieldsNotFound)
	return structFieldNameToArrowIndexMappings, err
}

// Recursive function to map struct field names to arrow field indices
func mapGoStructFieldNamesToArrowIndices(goStructType reflect.Type, goNamePrefix string, arrowFields []arrow.Field, goNamesToExclude []string, indicesIgnoreExcludedFields bool, goNameArrowIndexMap map[string]int, skipFieldsNotFound bool) error {
	for goStructType.Kind() == reflect.Pointer {
		goStructType = goStructType.Elem()
	}

	switch goStructType.Kind() {
	case reflect.Struct:
		skippedFields := 0
	processGoStructField:
		for i := 0; i < goStructType.NumField(); i++ {
			field := goStructType.Field(i)
			fieldName := nestedGoFieldName(goNamePrefix, field.Name)
			for _, exclude := range goNamesToExclude {
				if exclude == fieldName {
					skippedFields++
					continue processGoStructField
				}
			}
			// Default to using the field name as the parquet column name
			parquetName := field.Name

			// Look for a parquet tag for this field
			tag := field.Tag
			if ptags, ok := tag.Lookup("parquet"); ok {
				if ptags == "-" {
					// Omit
					// This is to support https://github.com/apache/arrow/issues/36793 (which will be in v14)
					parquetName = ""
				} else {
				findNameTag:
					for _, tag := range strings.Split(strings.Replace(ptags, "\t", "", -1), ",") {
						tag = strings.TrimSpace(tag)
						kv := strings.SplitN(tag, "=", 2)
						key := strings.TrimSpace(strings.ToLower(kv[0]))
						value := strings.TrimSpace(kv[1])

						switch key {
						case "name":
							parquetName = value
							break findNameTag
						default:
							// nop
						}
					}
				}

			}
			if len(parquetName) > 0 {
				var arrowField arrow.Field
				found := false
				for arrowIndex := 0; arrowIndex < len(arrowFields); arrowIndex++ {
					if arrowFields[arrowIndex].Name == parquetName {
						goNameArrowIndexMap[fieldName] = arrowIndex
						if indicesIgnoreExcludedFields {
							goNameArrowIndexMap[fieldName] -= skippedFields
						}
						arrowField = arrowFields[arrowIndex]
						found = true
						break
					}
				}
				if !found {
					if skipFieldsNotFound {
						continue processGoStructField
					}
					return fmt.Errorf("schema conversion error: could not find %s in arrow fields", parquetName)
				}

				arrowStructMemberFields := nestedArrowFields(arrowField)
				if arrowStructMemberFields != nil {
					err := mapGoStructFieldNamesToArrowIndices(field.Type, fieldName, arrowStructMemberFields, goNamesToExclude, indicesIgnoreExcludedFields, goNameArrowIndexMap, skipFieldsNotFound)
					if err != nil {
						return err
					}
				}
			}
		}
	case reflect.Array, reflect.Slice, reflect.Map:
		field := goStructType.Elem()
		arrowField := arrowFields[0]
		arrowStructMemberFields := nestedArrowFields(arrowField)
		if goStructType.Kind() == reflect.Map {
			// Get the map values. Keys aren't relevant for name indexing.
			arrowStructMemberFields = nestedArrowFields(arrowStructMemberFields[1])
		}
		if arrowStructMemberFields != nil {
			err := mapGoStructFieldNamesToArrowIndices(field, goNamePrefix, arrowStructMemberFields, goNamesToExclude, indicesIgnoreExcludedFields, goNameArrowIndexMap, skipFieldsNotFound)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Retrieve child fields from a nested type
func nestedArrowFields(arrowField arrow.Field) []arrow.Field {
	var arrowMemberFields []arrow.Field
	switch arrowField.Type.ID() {
	case arrow.STRUCT:
		arrowMemberFields = arrowField.Type.(*arrow.StructType).Fields()
	case arrow.LIST:
		arrowMemberFields = arrowField.Type.(*arrow.ListType).Fields()
	case arrow.MAP:
		arrowMemberFields = arrowField.Type.(*arrow.MapType).Fields()
	case arrow.FIXED_SIZE_LIST:
		arrowMemberFields = arrowField.Type.(*arrow.FixedSizeListType).Fields()
	}
	return arrowMemberFields
}
