package session

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func NewReaderSessionBuilder() ReaderSessionBuilder {
	resources := make([]io.Closer, 0)
	return ReaderSessionBuilder{resources: resources}
}

type ReaderSessionBuilder struct {
	resources []io.Closer
}

func (s *ReaderSessionBuilder) AttachResource(resource io.Closer) {
	s.resources = append(s.resources, resource)
}

func (s *ReaderSessionBuilder) NewBoundReader(rdr array.RecordReader) *SessionBoundRecordReader {
	newRdr := SessionBoundRecordReader{RecordReader: rdr, sessionResources: s.resources}
	s.resources = make([]io.Closer, 0) // Reset after 'move'
	return &newRdr
}

func (s *ReaderSessionBuilder) Close() {
	closeAllResources(s.resources)
}

type SessionBoundRecordReader struct {
	array.RecordReader
	sessionResources []io.Closer
}

func (qr *SessionBoundRecordReader) Release() {
	qr.RecordReader.Release()
	closeAllResources(qr.sessionResources)
}

func closeAllResources(resources []io.Closer) {
	// Close last resource first
	for i := len(resources) - 1; i >= 0; i-- {
		resources[i].Close()
	}
}
