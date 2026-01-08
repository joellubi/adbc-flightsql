package server

import (
	"github.com/joellubi/adbc-flightsql/server/session"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func NewServerFromDB(db adbc.Database, opts ...ServerOption) (*ADBCFlightSQLServer, error) {
	server := ADBCFlightSQLServer{db: &db}

	for _, opt := range opts {
		err := opt(&server)
		if err != nil {
			return nil, err
		}
	}

	return &server, nil
}

type ADBCFlightSQLServer struct {
	flightsql.BaseServer
	db *adbc.Database

	openReaders session.ReaderStore
}

type ServerOption func(*ADBCFlightSQLServer) error

func WithSyncReaderPersistence(enable bool) ServerOption {
	return func(srv *ADBCFlightSQLServer) error {
		if enable {
			srv.openReaders = session.NewReaderStore()
		}
		return nil
	}
}

func WithAllocator(mem memory.Allocator) ServerOption {
	return func(srv *ADBCFlightSQLServer) error {
		srv.Alloc = mem
		return nil
	}
}

func WithSqlInfo(id flightsql.SqlInfo, result any) ServerOption {
	return func(srv *ADBCFlightSQLServer) error {
		return srv.RegisterSqlInfo(id, result)
	}
}
