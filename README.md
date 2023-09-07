# rfarrow

rfarrow contains convenience functions for using the Arrow library in Go directly with structs.  It uses reflection to convert Go structs to and from Arrow format.

This is a pre-release version and the API may change.

## Installation

```
go get github.com/chelseajonesr/rfarrow
```

## How to Use

### Writing Go to Parquet

`WriteGoStructsToParquet` will write a slice of Go structs to an io.Writer in Parquet format:
```
err := WriteGoStructsToParquet(data, writer, nil)
```

You can set the compression type:
```
props := parquet.NewWriterProperties(
	parquet.WithCompression(compress.Codecs.Snappy),
)
err := WriteGoStructsToParquet(values, buf, props)
```

The Parquet schema is automatically generated from the struct type definition using functions from the Arrow library.
You can change the names and types of the Parquet columns by using the `parquet` tag in the struct type - for more details see the Arrow package:

```
type Add struct {
	Path            string            `parquet:"name=path, repetition=OPTIONAL, converted=UTF8"`
	PartitionValues map[string]string `parquet:"name=partitionValues, repetition=OPTIONAL, keyconverted=UTF8, valueconverted=UTF8"`
	TimeMilli       int32             `parquet:"name=timemilli, converted=TIME_MILLIS"`
}
```


### Reading Go from Parquet

`ReadGoStructsFromParquet` will read a Parquet file into a slice of Go struct pointers:
```
data, err := ReadGoStructsFromParquet[MyStructType](reader)
```

The `reader` needs to support `io.ReaderAt` and `io.Seeker`.

You can use the `parquet` tag in the struct definition to specify Parquet column names, logical types, etc.  For more details see the Arrow package.

### Converting Go structs to Arrow

`NewArrowTableFromGoStructs` will convert a slice of Go structs to an Arrow table:

```
tbl, err := NewArrowTableFromGoStructs(data)
if err != nil {
  ...
}
defer tbl.Release()
// Use tbl
```

## TODO

More examples