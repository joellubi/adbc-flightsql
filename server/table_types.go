package server

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
)

func (fss *ADBCFlightSQLServer) GetFlightInfoTableTypes(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.TableTypes

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

func (fss *ADBCFlightSQLServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	cnxn, err := (*fss.db).Open(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to open connection", err)
	}
	defer cnxn.Close()

	rdr, err := cnxn.GetTableTypes(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to get table types", err)
	}

	ch := make(chan flight.StreamChunk, 1)
	go flight.StreamChunksFromReader(rdr, ch)

	return rdr.Schema(), ch, nil
}
