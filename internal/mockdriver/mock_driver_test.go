package mockdriver

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/suite"
)

type MockDriverTestSuite struct {
	suite.Suite

	ctx context.Context
	db  adbc.Database
}

func (suite *MockDriverTestSuite) SetupSuite() {
	suite.ctx = context.TODO()
	drv := MockDriver{}
	db, err := drv.NewDatabase(nil)
	suite.NoError(err)
	suite.db = db
}

func (suite *MockDriverTestSuite) TestGetObjectsAll() {
	cnxn, err := suite.db.Open(suite.ctx)
	suite.NoError(err)

	rdr, err := cnxn.GetObjects(suite.ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	suite.NoError(err)

	table := tableFromReader(rdr)
	defer table.Release()

	expectedTable, err := array.TableFromJSON(memory.DefaultAllocator, adbc.GetObjectsSchema, []string{`[
		{
			"catalog_name": "empty",
			"catalog_db_schemas": null
		},
		{
			"catalog_name": "nonempty",
			"catalog_db_schemas": [
				{
					"db_schema_name": "empty",
					"db_schema_tables": null
				},
				{
					"db_schema_name": "nonempty",
					"db_schema_tables": [
						{
							"table_name": "test_table",
							"table_type": "table",
							"table_columns": [
								{
									"column_name": "col1",
									"ordinal_position": 1
								},
								{
									"column_name": "col2",
									"ordinal_position": 2
								}
							],
							"table_constraints": null
						}
					]
				}
			]
		}
	]`})
	suite.NoError(err)
	defer expectedTable.Release()

	suite.Truef(array.TableEqual(expectedTable, table), "expected: %s\ngot: %s", expectedTable, table)
}

func (suite *MockDriverTestSuite) TestGetObjectsCatalogs() {
	cnxn, err := suite.db.Open(suite.ctx)
	suite.NoError(err)

	rdr, err := cnxn.GetObjects(suite.ctx, adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
	suite.NoError(err)

	table := tableFromReader(rdr)
	defer table.Release()

	expectedTable, err := array.TableFromJSON(memory.DefaultAllocator, adbc.GetObjectsSchema, []string{`[
		{
			"catalog_name": "empty",
			"catalog_db_schemas": null
		},
		{
			"catalog_name": "nonempty",
			"catalog_db_schemas": null
		}
	]`})
	suite.NoError(err)
	defer expectedTable.Release()

	suite.Truef(array.TableEqual(expectedTable, table), "expected: %s\ngot: %s", expectedTable, table)
}

func (suite *MockDriverTestSuite) TestGetObjectsDBSchemas() {
	cnxn, err := suite.db.Open(suite.ctx)
	suite.NoError(err)

	rdr, err := cnxn.GetObjects(suite.ctx, adbc.ObjectDepthDBSchemas, nil, nil, nil, nil, nil)
	suite.NoError(err)

	table := tableFromReader(rdr)
	defer table.Release()

	expectedTable, err := array.TableFromJSON(memory.DefaultAllocator, adbc.GetObjectsSchema, []string{`[
		{
			"catalog_name": "empty",
			"catalog_db_schemas": null
		},
		{
			"catalog_name": "nonempty",
			"catalog_db_schemas": [
				{
					"db_schema_name": "empty",
					"db_schema_tables": null
				},
				{
					"db_schema_name": "nonempty",
					"db_schema_tables": null
				}
			]
		}
	]`})
	suite.NoError(err)
	defer expectedTable.Release()

	suite.Truef(array.TableEqual(expectedTable, table), "expected: %s\ngot: %s", expectedTable, table)
}

func (suite *MockDriverTestSuite) TestGetObjectsTables() {
	cnxn, err := suite.db.Open(suite.ctx)
	suite.NoError(err)

	rdr, err := cnxn.GetObjects(suite.ctx, adbc.ObjectDepthTables, nil, nil, nil, nil, nil)
	suite.NoError(err)

	table := tableFromReader(rdr)
	defer table.Release()

	expectedTable, err := array.TableFromJSON(memory.DefaultAllocator, adbc.GetObjectsSchema, []string{`[
		{
			"catalog_name": "empty",
			"catalog_db_schemas": null
		},
		{
			"catalog_name": "nonempty",
			"catalog_db_schemas": [
				{
					"db_schema_name": "empty",
					"db_schema_tables": null
				},
				{
					"db_schema_name": "nonempty",
					"db_schema_tables": [
						{
							"table_name": "test_table",
							"table_type": "table",
							"table_columns": null,
							"table_constraints": null
						}
					]
				}
			]
		}
	]`})
	suite.NoError(err)
	defer expectedTable.Release()

	suite.Truef(array.TableEqual(expectedTable, table), "expected: %s\ngot: %s", expectedTable, table)
}

func TestMockDriverTestSuite(t *testing.T) {
	suite.Run(t, new(MockDriverTestSuite))
}

func tableFromReader(rdr array.RecordReader) arrow.Table {
	recs := make([]arrow.RecordBatch, 0)
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		defer rec.Release()

		recs = append(recs, rec)
	}

	return array.NewTableFromRecords(rdr.Schema(), recs)
}
