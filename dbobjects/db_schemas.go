package dbobjects

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func NewDBSchemasReader(mem memory.Allocator, getObjectsReader array.RecordReader) array.RecordReader {
	targetSchema := schema_ref.DBSchemas
	transformer := func(rec arrow.RecordBatch) arrow.RecordBatch {
		catalogBuilder := array.NewStringBuilder(mem)
		dbSchemaBuilder := array.NewStringBuilder(mem)
		defer catalogBuilder.Release()
		defer dbSchemaBuilder.Release()

		catalog_name := rec.Column(0).(*array.String)
		catalog_db_schemas := rec.Column(1).(*array.List)

		var nrows int64
		for i := 0; i < catalog_db_schemas.Len(); i++ {
			beg, end := catalog_db_schemas.ValueOffsets(i)
			db_schema_name := array.NewSlice(catalog_db_schemas.ListValues(), beg, end).(*array.Struct).Field(0).(*array.String)
			for j := 0; j < db_schema_name.Len(); j++ {
				catalogBuilder.Append(catalog_name.Value(i))
				dbSchemaBuilder.Append(db_schema_name.Value(j))
				nrows++
			}
			db_schema_name.Release()
		}

		return array.NewRecordBatch(targetSchema, []arrow.Array{catalogBuilder.NewArray(), dbSchemaBuilder.NewArray()}, nrows)
	}

	return NewTransformingRecordReader(getObjectsReader, transformer, targetSchema)
}
