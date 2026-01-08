package server_test

import (
	"context"
	"testing"

	"github.com/joellubi/adbc-flightsql/internal/mockdriver"
	"github.com/joellubi/adbc-flightsql/server"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type ServerTestSuite struct {
	suite.Suite

	ctx context.Context
	srv flightsql.Server
}

func (suite *ServerTestSuite) SetupSuite() {
	suite.ctx = context.TODO()
	drv := mockdriver.MockDriver{}
	db, err := drv.NewDatabase(nil)
	suite.NoError(err)

	srv, err := server.NewServerFromDB(db, server.WithAllocator(memory.DefaultAllocator), server.WithSqlInfo(flightsql.SqlInfoFlightSqlServerName, "mock-server"))
	suite.NoError(err)

	suite.srv = srv
}

func (suite *ServerTestSuite) TestGetFlightInfoSqlInfo() {
	cmd := pb.CommandGetSqlInfo{}
	desc, err := descForCommand(&cmd)
	suite.NoError(err)

	_, err = suite.srv.GetFlightInfoSqlInfo(suite.ctx, &cmd, desc)
	suite.NoError(err)
}

func (suite *ServerTestSuite) TestDoGetSqlInfo() {
	cmd := pb.CommandGetSqlInfo{}
	schema, chunks, err := suite.srv.DoGetSqlInfo(suite.ctx, &cmd)
	suite.NoError(err)

	expectedSchema := schema_ref.SqlInfo
	suite.Truef(schema.Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, schema)

	var nSqlInfos int64
	for chunk := range chunks {
		suite.Truef(chunk.Data.Schema().Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, chunk.Data.Schema())
		nSqlInfos += chunk.Data.NumRows()
	}
	suite.Equal(int64(1), nSqlInfos)
}

func (suite *ServerTestSuite) TestGetFlightInfoStatement() {
	cmd := pb.CommandStatementQuery{Query: "SELECT * FROM nonempty.nonempty.test_table"}
	desc, err := descForCommand(&cmd)
	suite.NoError(err)

	info, err := suite.srv.GetFlightInfoStatement(suite.ctx, &cmd, desc)
	suite.NoError(err)

	suite.Len(info.Endpoint, 2)
	suite.Equal(int64(6), info.TotalRecords)
}

func (suite *ServerTestSuite) TestGetFlightInfoStatementInvalidQuery() {
	cmd := pb.CommandStatementQuery{Query: "SELECT * FROM nonempty.nonempty.doesnt_exist"}
	desc, err := descForCommand(&cmd)
	suite.NoError(err)

	_, err = suite.srv.GetFlightInfoStatement(suite.ctx, &cmd, desc)
	suite.ErrorContains(err, "invalid query")
}

func (suite *ServerTestSuite) TestDoGetStatement() {
	cmd := pb.TicketStatementQuery{StatementHandle: []byte("0")} // Simple index of record batch to retrieve
	schema, chunks, err := suite.srv.DoGetStatement(suite.ctx, &cmd)
	suite.NoError(err)

	expectedSchema := mockdriver.TestTableSchema
	suite.Truef(schema.Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, schema)

	var nChunks int
	for chunk := range chunks {
		suite.Truef(chunk.Data.Schema().Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, chunk.Data.Schema())
		suite.Equal(int64(3), chunk.Data.NumRows())
		nChunks++
	}
	suite.Equal(1, nChunks)
}

func (suite *ServerTestSuite) TestGetFlightInfoCatalogs() {
	cmd := pb.CommandGetCatalogs{}
	desc, err := descForCommand(&cmd)
	suite.NoError(err)

	_, err = suite.srv.GetFlightInfoCatalogs(suite.ctx, desc)
	suite.NoError(err)
}

func (suite *ServerTestSuite) TestDoGetCatalogs() {
	schema, chunks, err := suite.srv.DoGetCatalogs(suite.ctx)
	suite.NoError(err)

	expectedSchema := schema_ref.Catalogs
	suite.Truef(schema.Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, schema)

	var nCatalogs int64
	for chunk := range chunks {
		suite.Truef(chunk.Data.Schema().Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, chunk.Data.Schema())
		nCatalogs += chunk.Data.NumRows()
	}
	suite.Equal(int64(2), nCatalogs)
}

func (suite *ServerTestSuite) TestGetFlightInfoSchemas() {
	cmd := pb.CommandGetDbSchemas{}
	desc, err := descForCommand(&cmd)
	suite.NoError(err)

	_, err = suite.srv.GetFlightInfoSchemas(suite.ctx, &getDBSchemas{&cmd}, desc)
	suite.NoError(err)
}

func (suite *ServerTestSuite) TestDoGetDBSchemas() {
	cmd := pb.CommandGetDbSchemas{}
	schema, chunks, err := suite.srv.DoGetDBSchemas(suite.ctx, &getDBSchemas{&cmd})
	suite.NoError(err)

	expectedSchema := schema_ref.DBSchemas
	suite.Truef(schema.Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, schema)

	var nDBSchemas int64
	for chunk := range chunks {
		suite.Truef(chunk.Data.Schema().Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, chunk.Data.Schema())
		nDBSchemas += chunk.Data.NumRows()
	}
	suite.Equal(int64(2), nDBSchemas)
}

func (suite *ServerTestSuite) TestGetFlightInfoTables() {
	cmd := pb.CommandGetTables{}
	desc, err := descForCommand(&cmd)
	suite.NoError(err)

	_, err = suite.srv.GetFlightInfoTables(suite.ctx, &getTables{&cmd}, desc)
	suite.NoError(err)
}

func (suite *ServerTestSuite) TestDoGetTables() {
	cmd := pb.CommandGetTables{}
	schema, chunks, err := suite.srv.DoGetTables(suite.ctx, &getTables{&cmd})
	suite.NoError(err)

	expectedSchema := schema_ref.Tables
	suite.Truef(schema.Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, schema)

	var nTables int64
	for chunk := range chunks {
		suite.Truef(chunk.Data.Schema().Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, chunk.Data.Schema())
		nTables += chunk.Data.NumRows()
	}
	suite.Equal(int64(1), nTables)
}

func (suite *ServerTestSuite) TestGetFlightInfoTablesWithIncludedSchema() {
	cmd := pb.CommandGetTables{IncludeSchema: true}
	desc, err := descForCommand(&cmd)
	suite.NoError(err)

	_, err = suite.srv.GetFlightInfoTables(suite.ctx, &getTables{&cmd}, desc)
	suite.NoError(err)
}

func (suite *ServerTestSuite) TestDoGetTablesWithIncludedSchema() {
	cmd := pb.CommandGetTables{IncludeSchema: true}
	schema, chunks, err := suite.srv.DoGetTables(suite.ctx, &getTables{&cmd})
	suite.NoError(err)

	expectedSchema := schema_ref.TablesWithIncludedSchema
	suite.Truef(schema.Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, schema)

	var nTables int64
	for chunk := range chunks {
		suite.NoError(chunk.Err)

		suite.Truef(chunk.Data.Schema().Equal(expectedSchema), "expected: %s\ngot: %s", expectedSchema, chunk.Data.Schema())
		nTables += chunk.Data.NumRows()
	}
	suite.Equal(int64(1), nTables)
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}

func descForCommand(cmd proto.Message) (*flight.FlightDescriptor, error) {
	var any anypb.Any
	if err := any.MarshalFrom(cmd); err != nil {
		return nil, err
	}

	data, err := proto.Marshal(&any)
	if err != nil {
		return nil, err
	}
	return &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  data,
	}, nil
}

type getDBSchemas struct {
	*pb.CommandGetDbSchemas
}

func (c *getDBSchemas) GetCatalog() *string               { return c.Catalog }
func (c *getDBSchemas) GetDBSchemaFilterPattern() *string { return c.DbSchemaFilterPattern }

type getTables struct {
	*pb.CommandGetTables
}

func (c *getTables) GetCatalog() *string                { return c.Catalog }
func (c *getTables) GetDBSchemaFilterPattern() *string  { return c.DbSchemaFilterPattern }
func (c *getTables) GetTableNameFilterPattern() *string { return c.TableNameFilterPattern }
