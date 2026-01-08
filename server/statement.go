package server

import (
	"context"
	"fmt"

	"github.com/joellubi/adbc-flightsql/server/session"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type statementQueryOrSubstraitPlan struct {
	query         flightsql.StatementQuery
	substraitPlan flightsql.StatementSubstraitPlan
}

func NewStatementQuery(query flightsql.StatementQuery) statementQueryOrSubstraitPlan {
	return statementQueryOrSubstraitPlan{query: query}
}

func NewStatementSubstraitPlan(substraitPlan flightsql.StatementSubstraitPlan) statementQueryOrSubstraitPlan {
	return statementQueryOrSubstraitPlan{substraitPlan: substraitPlan}
}

func (stmt *statementQueryOrSubstraitPlan) GetQuery() (string, bool) {
	if stmt.query != nil {
		return stmt.query.GetQuery(), true
	}

	return "", false
}

func (stmt *statementQueryOrSubstraitPlan) GetPlan() (flightsql.SubstraitPlan, bool) {
	if stmt.substraitPlan != nil {
		return stmt.substraitPlan.GetPlan(), true
	}

	return flightsql.SubstraitPlan{}, false
}

func (stmt *statementQueryOrSubstraitPlan) GetTransactionId() ([]byte, bool) {
	if stmt.query != nil {
		return stmt.query.GetTransactionId(), true
	}

	if stmt.substraitPlan != nil {
		return stmt.substraitPlan.GetTransactionId(), true
	}

	return nil, false
}

func (fss *ADBCFlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if fss.openReaders == nil {
		return fss.getFlightInfoStatement(ctx, NewStatementQuery(cmd), desc)
	}
	return fss.getFlightInfoStatementAndPersistReader(ctx, NewStatementQuery(cmd), desc)
}

func (fss *ADBCFlightSQLServer) GetFlightInfoSubstraitPlan(ctx context.Context, cmd flightsql.StatementSubstraitPlan, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if fss.openReaders == nil {
		return fss.getFlightInfoStatement(ctx, NewStatementSubstraitPlan(cmd), desc)
	}
	return fss.getFlightInfoStatementAndPersistReader(ctx, NewStatementSubstraitPlan(cmd), desc)
}

func (fss *ADBCFlightSQLServer) getFlightInfoStatement(ctx context.Context, cmd statementQueryOrSubstraitPlan, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if txID, _ := cmd.GetTransactionId(); txID != nil {
		return nil, status.Error(codes.Unimplemented, "transactions are unsupported")
	}

	cnxn, err := (*fss.db).Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open connection", err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open statement", err)
	}
	defer stmt.Close()

	if query, ok := cmd.GetQuery(); ok {
		if err = stmt.SetSqlQuery(query); err != nil {
			return nil, fmt.Errorf("failed to set sql query: %w", err)
		}
	}

	if plan, ok := cmd.GetPlan(); ok {
		// TODO: Handle version
		if err = stmt.SetSubstraitPlan(plan.Plan); err != nil {
			return nil, fmt.Errorf("failed to set substrait plan: %w", err)
		}
	}

	schema, partitions, nrows, err := stmt.ExecutePartitions(ctx)
	if err != nil {
		// return nil, status.Errorf(codes.Internal, "%wfailed to execute partitioned query", err)
		return nil, fmt.Errorf("%w: failed to execute partitioned query", err)
	}

	endpoints := make([]*flight.FlightEndpoint, partitions.NumPartitions)
	for i, partitionID := range partitions.PartitionIDs {
		ticket, err := flightsql.CreateStatementQueryTicket(partitionID)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to create StatementQueryTicket", err)
		}
		endpoints[i] = &flight.FlightEndpoint{Ticket: &flight.Ticket{Ticket: ticket}}
	}

	return &flight.FlightInfo{
		Endpoint:         endpoints,
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, fss.Alloc),
		TotalRecords:     nrows,
		TotalBytes:       -1,
	}, nil
}

func (fss *ADBCFlightSQLServer) getFlightInfoStatementAndPersistReader(ctx context.Context, cmd statementQueryOrSubstraitPlan, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	rdrSessionBldr := session.NewReaderSessionBuilder()
	defer rdrSessionBldr.Close()

	if txID, _ := cmd.GetTransactionId(); txID != nil {
		return nil, status.Error(codes.Unimplemented, "transactions are unsupported")
	}

	cnxn, err := (*fss.db).Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open connection", err)
	}
	rdrSessionBldr.AttachResource(cnxn)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open statement", err)
	}
	rdrSessionBldr.AttachResource(stmt)

	if query, ok := cmd.GetQuery(); ok {
		if err = stmt.SetSqlQuery(query); err != nil {
			return nil, fmt.Errorf("failed to set sql query: %w", err)
		}
	}

	if plan, ok := cmd.GetPlan(); ok {
		// TODO: Handle version
		if err = stmt.SetSubstraitPlan(plan.Plan); err != nil {
			return nil, fmt.Errorf("failed to set substrait plan: %w", err)
		}
	}

	rdr, nrows, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to execute query", err)
	}

	rdr = rdrSessionBldr.NewBoundReader(rdr)
	ticket, err := fss.openReaders.Push(rdr)
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(rdr.Schema(), fss.Alloc),
		TotalRecords:     nrows,
		TotalBytes:       -1,
	}, nil
}

func (fss *ADBCFlightSQLServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if fss.openReaders == nil {
		return fss.doGetStatement(ctx, cmd)
	}
	return fss.doGetStatementWithPersistedReader(ctx, cmd)
}

func (fss *ADBCFlightSQLServer) doGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	ticket := cmd.GetStatementHandle()

	cnxn, err := (*fss.db).Open(ctx)
	if err != nil {
		// TODO: GRPC errors?
		return nil, nil, fmt.Errorf("%w: failed to open connection", err)
	}
	defer cnxn.Close()

	rdr, err := cnxn.ReadPartition(ctx, ticket)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to read partition results", err)
	}

	ch := make(chan flight.StreamChunk, 1)
	go flight.StreamChunksFromReader(rdr, ch)

	return rdr.Schema(), ch, nil
}

func (fss *ADBCFlightSQLServer) doGetStatementWithPersistedReader(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	ticket := cmd.GetStatementHandle()

	rdr, err := fss.openReaders.Pop(ticket)
	if err != nil {
		return nil, nil, status.Error(codes.Internal, err.Error())
	}

	ch := make(chan flight.StreamChunk, 1)
	go flight.StreamChunksFromReader(rdr, ch)

	return rdr.Schema(), ch, nil
}

func (fss *ADBCFlightSQLServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	var nrows int64

	if cmd.GetTransactionId() != nil {
		return nrows, status.Error(codes.Unimplemented, "transactions are unsupported")
	}
	query := cmd.GetQuery()

	cnxn, err := (*fss.db).Open(ctx)
	if err != nil {
		return nrows, fmt.Errorf("%w: failed to open connection", err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		return nrows, fmt.Errorf("%w: failed to open statement", err)
	}
	defer stmt.Close()

	err = stmt.SetSqlQuery(query)
	if err != nil {
		return nrows, fmt.Errorf("%w: failed to set sql query", err)
	}

	nrows, err = stmt.ExecuteUpdate(ctx)
	if err != nil {
		return nrows, fmt.Errorf("%w: failed to execute query", err)
	}

	return nrows, nil
}
