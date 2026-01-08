package session

import (
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/google/uuid"
)

const DefaultReaderTTL time.Duration = 10 * time.Second

type ReaderStore interface {
	Push(rdr array.RecordReader) (ticket []byte, err error)
	Pop(ticket []byte) (rdr array.RecordReader, err error)
}

func NewReaderStore() *readerStore {
	return &readerStore{ttl: DefaultReaderTTL}
}

type readerStoreImpl struct {
	sync.RWMutex
	readers map[string]array.RecordReader
	ttl     time.Duration
}

func (rs *readerStoreImpl) Push(rdr array.RecordReader) ([]byte, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	key := id.String()
	ticket, err := encodeTicket(key)
	if err != nil {
		return nil, err
	}

	rs.Lock()
	defer rs.Unlock()
	rs.readers[key] = rdr
	go rs.cleanupReaderAfterTTL(key)

	return ticket, nil
}

func (rs *readerStoreImpl) cleanupReaderAfterTTL(key string) {
	time.Sleep(rs.ttl)
	rs.Lock()
	defer rs.Unlock()
	rdr, ok := rs.readers[key]
	if ok {
		rdr.Release()
	}
	delete(rs.readers, key)
}

func (rs *readerStoreImpl) Pop(ticket []byte) (array.RecordReader, error) {
	key, err := decodeTicket(ticket)
	if err != nil {
		return nil, err
	}

	rs.RLock()
	defer rs.RUnlock()
	rdr, ok := rs.readers[key]
	if !ok {
		return nil, fmt.Errorf("reader at key %s not found", key)
	}
	if rdr == nil {
		return nil, fmt.Errorf("nil reader found at key %s", key)
	}
	delete(rs.readers, key)

	return rdr, nil
}

type readerStore struct {
	readers sync.Map
	ttl     time.Duration
}

func (rs *readerStore) Push(rdr array.RecordReader) ([]byte, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	key := id.String()
	ticket, err := encodeTicket(key)
	if err != nil {
		return nil, err
	}

	_, loaded := rs.readers.LoadOrStore(key, rdr)
	if loaded {
		return nil, fmt.Errorf("reader already stored at key %s", key)
	}

	go rs.cleanupReaderAfterTTL(key)

	return ticket, nil
}

func (rs *readerStore) Pop(ticket []byte) (array.RecordReader, error) {
	key, err := decodeTicket(ticket)
	if err != nil {
		return nil, err
	}

	val, loaded := rs.readers.LoadAndDelete(key)
	if !loaded {
		return nil, fmt.Errorf("reader at key %s not found", key)
	}

	rdr, ok := val.(array.RecordReader)
	if !ok {
		return nil, fmt.Errorf("reader at key %s is invalid: %s", key, val)
	}

	return rdr, nil
}

func (rs *readerStore) cleanupReaderAfterTTL(key string) {
	time.Sleep(rs.ttl)
	val, loaded := rs.readers.LoadAndDelete(key)
	if loaded {
		rdr, ok := val.(array.RecordReader)
		if !ok {
			fmt.Printf("unable to cleanup key %s, reader is invalid: %s", key, val)
			return
		}
		rdr.Release()
	}
}

func encodeTicket(key string) ([]byte, error) {
	in := []byte(key)
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(in)))
	base64.StdEncoding.Encode(encoded, in)
	return flightsql.CreateStatementQueryTicket(encoded)
}

func decodeTicket(ticket []byte) (string, error) {
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(ticket)))
	n, err := base64.StdEncoding.Decode(decoded, ticket)
	if err != nil {
		return "", err
	}
	decoded = decoded[:n] // Trim unused bytes
	return string(decoded), nil
}
