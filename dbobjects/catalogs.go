package dbobjects

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
)

func NewCatalogsReader(getObjectsReader array.RecordReader) array.RecordReader {
	targetSchema := schema_ref.Catalogs
	transformer := func(rec arrow.RecordBatch) arrow.RecordBatch {
		return array.NewRecordBatch(targetSchema, []arrow.Array{rec.Column(0)}, rec.NumRows())
	}

	return NewTransformingRecordReader(getObjectsReader, transformer, targetSchema)
}
