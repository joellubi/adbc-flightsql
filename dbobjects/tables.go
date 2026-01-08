package dbobjects

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func NewTablesReader(mem memory.Allocator, getObjectsReader array.RecordReader) array.RecordReader {
	targetSchema := schema_ref.Tables

	transformer := func(rec arrow.RecordBatch) arrow.RecordBatch {
		return transformGetObjectsToTables(mem, rec)
	}

	return NewTransformingRecordReader(getObjectsReader, transformer, targetSchema)
}

func NewTablesWithSchemaReader(mem memory.Allocator, getObjectsReader array.RecordReader, cnxn adbc.Connection) array.RecordReader {
	targetSchema := schema_ref.TablesWithIncludedSchema

	transformer := func(rec arrow.RecordBatch) arrow.RecordBatch {
		tablesRec := transformGetObjectsToTables(mem, rec)
		return addTableSchemaColumnToRecord(mem, tablesRec, cnxn)
	}

	return NewTransformingRecordReader(getObjectsReader, transformer, targetSchema)
}

func transformGetObjectsToTables(mem memory.Allocator, rec arrow.RecordBatch) arrow.RecordBatch {
	catalogBuilder := array.NewStringBuilder(mem)
	dbSchemaBuilder := array.NewStringBuilder(mem)
	tableNameBuilder := array.NewStringBuilder(mem)
	tableTypeBuilder := array.NewStringBuilder(mem)
	// tableSchemaBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary) // Maybe conditional?
	defer catalogBuilder.Release()
	defer dbSchemaBuilder.Release()
	defer tableNameBuilder.Release()
	defer tableTypeBuilder.Release()
	// defer tableSchemaBuilder.Release()

	catalog_name := rec.Column(0).(*array.String)
	catalog_db_schemas := rec.Column(1).(*array.List)

	var nrows int64
	for i := 0; i < catalog_db_schemas.Len(); i++ {
		beg, end := catalog_db_schemas.ValueOffsets(i)
		catalog_db_schema := array.NewSlice(catalog_db_schemas.ListValues(), beg, end).(*array.Struct)
		db_schema_name := catalog_db_schema.Field(0).(*array.String)
		db_schema_tables := catalog_db_schema.Field(1).(*array.List)
		for j := 0; j < db_schema_tables.Len(); j++ {
			beg, end := db_schema_tables.ValueOffsets(j)
			db_schema_table := array.NewSlice(db_schema_tables.ListValues(), beg, end).(*array.Struct)
			tableName := db_schema_table.Field(0).(*array.String)
			tableType := db_schema_table.Field(1).(*array.String)
			// tableColumns := db_schema_table.Field(2).(*array.List)
			for k := 0; k < tableName.Len(); k++ {
				catalogBuilder.Append(catalog_name.Value(i))
				dbSchemaBuilder.Append(db_schema_name.Value(j))
				tableNameBuilder.Append(tableName.Value(k))
				tableTypeBuilder.Append(tableType.Value(k))

				// if includeSchema {
				// 	columnFields := make([]arrow.Field, 0)
				// 	for l := 0; l < tableColumns.Len(); l++ {
				// 		beg, end := tableColumns.ValueOffsets(l)
				// 		tableColumn := array.NewSlice(tableColumns.ListValues(), beg, end).(*array.Struct)
				// 		columnName := tableColumn.Field(0).(*array.String)
				// 		columnFields = append(columnFields, arrow.Field{Name: columnName.Value(l)})
				// 	}
				// 	tableSchemaBuilder.Append(flight.SerializeSchema(arrow.NewSchema(columnFields, nil), mem))
				// }

				nrows++
			}
			db_schema_table.Release()
		}
		db_schema_name.Release()
	}

	return array.NewRecordBatch(schema_ref.Tables, []arrow.Array{catalogBuilder.NewArray(), dbSchemaBuilder.NewArray(), tableNameBuilder.NewArray(), tableTypeBuilder.NewArray()}, nrows)
}

func addTableSchemaColumnToRecord(mem memory.Allocator, rec arrow.RecordBatch, cnxn adbc.Connection) arrow.RecordBatch {
	// defer rec.Release()

	colsIn := rec.Columns()
	idxAppend := len(colsIn)
	colsOut := make([]arrow.Array, idxAppend+1)

	copy(colsOut, colsIn)

	tableSchemaBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer tableSchemaBuilder.Release()

	catalogNames := rec.Column(0).(*array.String)
	dbSchemaNames := rec.Column(1).(*array.String)
	tableNames := rec.Column(2).(*array.String)
	for i := 0; i < tableNames.Len(); i++ {
		catalogName := catalogNames.Value(i)
		dbSchemaName := dbSchemaNames.Value(i)
		tableName := tableNames.Value(i)

		schema, err := cnxn.GetTableSchema(context.TODO(), &catalogName, &dbSchemaName, tableName)
		if err != nil {
			fmt.Println("unhandled err:", err)
			return nil // TODO
		}

		tableSchemaBuilder.Append(flight.SerializeSchema(schema, mem))

	}
	colsOut[idxAppend] = tableSchemaBuilder.NewArray()

	return array.NewRecordBatch(schema_ref.TablesWithIncludedSchema, colsOut, rec.NumRows())
}

// func addColumnToRecord(mem memory.Allocator, rec arrow.Record, col arrow.Array) arrow.Record {
// 	defer rec.Release()

// 	colsIn := rec.Columns()
// 	colsOut := make([]arrow.Array, 0, len(colsIn)+1)

// 	copy(colsOut, colsIn)
// 	colsOut = append(colsOut, col)

// 	return array.NewRecord(schema_ref.TablesWithIncludedSchema, colsOut, int64(arr.Len()))
// }
