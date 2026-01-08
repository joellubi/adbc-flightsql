package server

import (
	"context"
	"fmt"

	"github.com/joellubi/adbc-flightsql/dbobjects"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
)

func (fss *ADBCFlightSQLServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}

	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: desc.Cmd}},
		},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, fss.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (fss *ADBCFlightSQLServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	includeSchema := cmd.GetIncludeSchema()
	if includeSchema {
		return fss.doGetTablesWithIncludedSchema(ctx, cmd)
	}
	return fss.doGetTables(ctx, cmd)
}

func (fss *ADBCFlightSQLServer) doGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	catalog := cmd.GetCatalog()
	dbSchema := cmd.GetDBSchemaFilterPattern()
	tableName := cmd.GetTableNameFilterPattern()
	tableType := cmd.GetTableTypes()
	includeSchema := cmd.GetIncludeSchema()
	if includeSchema {
		return nil, nil, fmt.Errorf("expected includeSchema to be false")
	}

	cnxn, err := (*fss.db).Open(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to open connection", err)
	}
	defer cnxn.Close() // TODO: Close in goroutine?

	rdr, err := cnxn.GetObjects(ctx, adbc.ObjectDepthTables, catalog, dbSchema, tableName, nil, tableType)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to get objects", err)
	}

	tablesReader := dbobjects.NewTablesReader(fss.Alloc, rdr)
	ch := make(chan flight.StreamChunk, 1)
	go flight.StreamChunksFromReader(tablesReader, ch)

	return tablesReader.Schema(), ch, nil
}

func (fss *ADBCFlightSQLServer) doGetTablesWithIncludedSchema(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	catalog := cmd.GetCatalog()
	dbSchema := cmd.GetDBSchemaFilterPattern()
	tableName := cmd.GetTableNameFilterPattern()
	tableType := cmd.GetTableTypes()
	includeSchema := cmd.GetIncludeSchema()
	if !includeSchema {
		return nil, nil, fmt.Errorf("expected includeSchema to be true")
	}

	cnxn, err := (*fss.db).Open(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to open connection", err)
	}

	rdr, err := cnxn.GetObjects(ctx, adbc.ObjectDepthColumns, catalog, dbSchema, tableName, nil, tableType)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to get objects", err)
	}

	// tablesReader := reader.NewTablesReader(fss.Alloc, rdr, true)
	tablesReader := dbobjects.NewTablesWithSchemaReader(fss.Alloc, rdr, cnxn)
	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer cnxn.Close()
		flight.StreamChunksFromReader(tablesReader, ch)
	}()

	return tablesReader.Schema(), ch, nil
}
