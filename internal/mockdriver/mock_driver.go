package mockdriver

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	TestTableName string = "nonempty.nonempty.test_table"
)

var (
	AcceptedQuery string = fmt.Sprintf("SELECT * FROM %s", TestTableName)
)

type MockDriver struct{}

func (drv MockDriver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	db := NewTestDatabase()
	return &MockDatabase{db: db}, nil
}

type MockDatabase struct {
	db *TestDatabase
}

func (db *MockDatabase) Close() error {
	return nil
}

func (db *MockDatabase) SetOptions(map[string]string) error {
	return nil
}

func (db *MockDatabase) Open(ctx context.Context) (adbc.Connection, error) {
	return &MockConnection{db: db.db}, nil
}

type MockConnection struct {
	db *TestDatabase
}

func (cnxn *MockConnection) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	return nil, fmt.Errorf("GetInfo is unimplemented")
}

func (cnxn *MockConnection) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, adbc.GetObjectsSchema)
	defer bldr.Release()

	// GetObjectsSchema
	catalogNameBldr := bldr.Field(0).(*array.StringBuilder)
	catalogDBSchemasBldr := bldr.Field(1).(*array.ListBuilder)
	catalogDBSchemaBldr := catalogDBSchemasBldr.ValueBuilder().(*array.StructBuilder)

	// DBSchemaSchema
	dbSchemaNameBldr := catalogDBSchemaBldr.FieldBuilder(0).(*array.StringBuilder)
	dbSchemaTablesBldr := catalogDBSchemaBldr.FieldBuilder(1).(*array.ListBuilder)
	dbSchemaTableBldr := dbSchemaTablesBldr.ValueBuilder().(*array.StructBuilder)

	// TableSchema
	tableNameBldr := dbSchemaTableBldr.FieldBuilder(0).(*array.StringBuilder)
	tableTypeBldr := dbSchemaTableBldr.FieldBuilder(1).(*array.StringBuilder)
	tableColumnsBldr := dbSchemaTableBldr.FieldBuilder(2).(*array.ListBuilder)
	tableColumnBldr := tableColumnsBldr.ValueBuilder().(*array.StructBuilder)
	tableConstraintsBldr := dbSchemaTableBldr.FieldBuilder(3).(*array.ListBuilder)
	// tableConstraintBldr := tableConstraintsBldr.ValueBuilder().(*array.StructBuilder)

	// ColumnSchema
	columnNameBldr := tableColumnBldr.FieldBuilder(0).(*array.StringBuilder)
	columnOrdinalBldr := tableColumnBldr.FieldBuilder(1).(*array.Int32Builder)

	// ConstraintSchema
	// TODO

	for _, catalog := range cnxn.db.Catalogs {
		catalogNameBldr.Append(catalog.Name)
		if depth == adbc.ObjectDepthCatalogs {
			catalogDBSchemasBldr.AppendNull()
			continue
		}

		catalogDBSchemasBldr.Append(len(catalog.DBSchemas) > 0)
		for _, dbSchema := range catalog.DBSchemas {
			catalogDBSchemaBldr.Append(true)
			dbSchemaNameBldr.Append(dbSchema.Name)
			if depth == adbc.ObjectDepthDBSchemas {
				dbSchemaTablesBldr.AppendNull()
				continue
			}

			dbSchemaTablesBldr.Append(len(dbSchema.Tables) > 0)
			for _, table := range dbSchema.Tables {
				dbSchemaTableBldr.Append(true)
				tableNameBldr.Append(table.Name)
				tableTypeBldr.Append(table.Type)
				if depth == adbc.ObjectDepthTables {
					tableColumnsBldr.AppendNull()
					tableConstraintsBldr.AppendNull()
					continue
				}

				arrowTable := table.Get()
				defer arrowTable.Release()

				tableFields := arrowTable.Schema().Fields()
				tableColumnsBldr.Append(len(tableFields) > 0)
				for i, field := range tableFields {
					tableColumnBldr.Append(true)
					columnNameBldr.Append(field.Name)
					columnOrdinalBldr.Append(int32(i + 1))

					// TODO
					tableColumnBldr.FieldBuilder(2).AppendNull()
					tableColumnBldr.FieldBuilder(3).AppendNull()
					tableColumnBldr.FieldBuilder(4).AppendNull()
					tableColumnBldr.FieldBuilder(5).AppendNull()
					tableColumnBldr.FieldBuilder(6).AppendNull()
					tableColumnBldr.FieldBuilder(7).AppendNull()
					tableColumnBldr.FieldBuilder(8).AppendNull()
					tableColumnBldr.FieldBuilder(9).AppendNull()
					tableColumnBldr.FieldBuilder(10).AppendNull()
					tableColumnBldr.FieldBuilder(11).AppendNull()
					tableColumnBldr.FieldBuilder(12).AppendNull()
					tableColumnBldr.FieldBuilder(13).AppendNull()
					tableColumnBldr.FieldBuilder(14).AppendNull()
					tableColumnBldr.FieldBuilder(15).AppendNull()
					tableColumnBldr.FieldBuilder(16).AppendNull()
					tableColumnBldr.FieldBuilder(17).AppendNull()
					tableColumnBldr.FieldBuilder(18).AppendNull()
				}
				tableConstraintsBldr.AppendNull() // TODO
			}
		}
	}

	rec := bldr.NewRecordBatch()
	return array.NewRecordReader(adbc.GetObjectsSchema, []arrow.RecordBatch{rec})
}

func (cnxn *MockConnection) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	currentCatalog := "nonempty"
	currentDBSchema := "nonempty"
	if catalog == nil {
		currentCatalog = *catalog
	}
	if dbSchema == nil {
		currentDBSchema = *dbSchema
	}

	fqTableName := fmt.Sprintf("%s.%s.%s", currentCatalog, currentDBSchema, tableName)
	if fqTableName != TestTableName {
		return nil, fmt.Errorf("invalid table selected: %s", fqTableName)
	}

	table, err := getTestTable(cnxn.db)
	if err != nil {
		return nil, err
	}

	return table.Schema(), nil
}

func (cnxn *MockConnection) GetTableTypes(context.Context) (array.RecordReader, error) {
	schema := adbc.TableTypesSchema

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.StringBuilder).Append("table")
	bldr.Field(0).(*array.StringBuilder).Append("view")

	rec := bldr.NewRecordBatch()

	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		return nil, err
	}

	return rdr, nil
}

func (cnxn *MockConnection) Commit(context.Context) error {
	return fmt.Errorf("Commit is unimplemented")
}

func (cnxn *MockConnection) Rollback(context.Context) error {
	return fmt.Errorf("Rollback is unimplemented")
}

func (cnxn *MockConnection) NewStatement() (adbc.Statement, error) {
	return &MockStatement{db: cnxn.db}, nil
}

func (cnxn *MockConnection) Close() error {
	return nil
}

func (cnxn *MockConnection) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	idx, err := strconv.Atoi(string(serializedPartition))
	if err != nil {
		return nil, err
	}

	if idx < 0 || idx > 1 {
		return nil, fmt.Errorf("partition index %d out of bounds", idx)
	}

	table := cnxn.db.Catalogs[1].DBSchemas[1].Tables[0].Get()
	rdr := array.NewTableReader(table, table.NumRows()/2)
	defer rdr.Release()

	// var i int
	recs := make([]arrow.RecordBatch, 0)
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		defer rec.Release()
		recs = append(recs, rec)
		// if i == idx {
		// 	rec.Retain()
		// 	return array.NewRecordReader(rec.Schema(), []arrow.Record{rec})
		// }
		// i++
	}

	if len(recs) != 2 {
		panic("Expected exactly 2 record batches")
	}

	rec := recs[idx]
	return array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
}

type MockStatement struct {
	db    *TestDatabase
	query string
}

func (s *MockStatement) Close() error {
	return nil
}

func (s *MockStatement) SetOption(key, val string) error {
	return nil
}

func (s *MockStatement) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

func (s *MockStatement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	table, err := s.getTableForQuery()
	if err != nil {
		return nil, 0, err
	}

	return array.NewTableReader(table, 0), table.NumRows(), nil
}

func (s *MockStatement) ExecuteUpdate(ctx context.Context) (int64, error) {
	return 0, fmt.Errorf("unimplemented ExecuteUpdate")
}

func (s *MockStatement) Prepare(context.Context) error {
	return fmt.Errorf("unimplemented Prepare")
}

func (s *MockStatement) SetSubstraitPlan(plan []byte) error {
	return fmt.Errorf("unimplemented SetSubstraitPlan")
}

func (s *MockStatement) Bind(ctx context.Context, values arrow.RecordBatch) error {
	return fmt.Errorf("unimplemented Bind")
}

func (s *MockStatement) BindStream(ctx context.Context, stream array.RecordReader) error {
	return fmt.Errorf("unimplemented BindStream")
}

func (s *MockStatement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, fmt.Errorf("unimplemented GetParameterSchema")
}

func (s *MockStatement) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	table, err := s.getTableForQuery()
	if err != nil {
		return nil, adbc.Partitions{}, 0, err
	}

	return table.Schema(), adbc.Partitions{NumPartitions: 2, PartitionIDs: [][]byte{[]byte("0"), []byte("1")}}, 6, nil
}

func (s *MockStatement) getTableForQuery() (arrow.Table, error) {
	if s.query == "" {
		return nil, fmt.Errorf("must call SetSqlQuery before ExecuteQuery")
	}
	if s.query != AcceptedQuery {
		return nil, fmt.Errorf("invalid query %s", s.query)
	}

	return getTestTable(s.db)
}

func getTestTable(db *TestDatabase) (arrow.Table, error) {
	return db.Catalogs[1].DBSchemas[1].Tables[0].Get(), nil
}
