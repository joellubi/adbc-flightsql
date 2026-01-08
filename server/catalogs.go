package server

import (
	"context"
	"fmt"

	"github.com/joellubi/adbc-flightsql/dbobjects"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
)

func (fss *ADBCFlightSQLServer) GetFlightInfoCatalogs(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: desc.Cmd}},
		},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema_ref.Catalogs, fss.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (fss *ADBCFlightSQLServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	cnxn, err := (*fss.db).Open(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to open connection", err)
	}
	defer cnxn.Close()

	rdr, err := cnxn.GetObjects(ctx, adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to get objects", err)
	}

	catalogsReader := dbobjects.NewCatalogsReader(rdr)
	ch := make(chan flight.StreamChunk, 1)
	go flight.StreamChunksFromReader(catalogsReader, ch)

	return catalogsReader.Schema(), ch, nil
}
