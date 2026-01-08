package mockdriver

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var TestTableSchema = arrow.NewSchema([]arrow.Field{
	{Name: "col1", Type: arrow.PrimitiveTypes.Int64},
	{Name: "col2", Type: arrow.BinaryTypes.String},
}, nil)

func NewTestDatabase() *TestDatabase {
	return &TestDatabase{
		Catalogs: []Catalog{
			{
				Name: "empty",
			},
			{
				Name: "nonempty",
				DBSchemas: []DBSchema{
					{
						Name: "empty",
					},
					{
						Name: "nonempty",
						Tables: []Table{
							{
								Name:     "test_table",
								Type:     "table",
								getTable: createTestTable,
							},
						},
					},
				},
			},
		},
	}
}

type Table struct {
	Name     string
	Type     string
	getTable func() arrow.Table
}

func (t Table) Get() arrow.Table {
	return t.getTable()
}

type DBSchema struct {
	Name   string
	Tables []Table
}

type Catalog struct {
	Name      string
	DBSchemas []DBSchema
}

type TestDatabase struct {
	Catalogs []Catalog
}

func createTestTable() arrow.Table {
	int64Bldr := array.NewInt64Builder(memory.DefaultAllocator)
	stringBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	defer int64Bldr.Release()
	defer stringBuilder.Release()

	int64Bldr.AppendValues([]int64{1, 2, 3, 4, 5, 6}, nil)
	stringBuilder.AppendValues([]string{"one", "two", "three", "four", "five", "six"}, nil)

	return array.NewTable(
		TestTableSchema,
		[]arrow.Column{
			arrow.NewColumnFromArr(TestTableSchema.Field(0), int64Bldr.NewArray()),
			arrow.NewColumnFromArr(TestTableSchema.Field(1), stringBuilder.NewArray()),
		},
		6,
	)
}
