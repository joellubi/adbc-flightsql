package dbobjects

import (
	"errors"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type RecordTransformer func(arrow.RecordBatch) arrow.RecordBatch

// Will release rdr when released
func NewTransformingRecordReader(rdr array.RecordReader, transformer RecordTransformer, schema *arrow.Schema) *TransformingRecordReader {
	return &TransformingRecordReader{rdr: rdr, transformer: transformer, targetSchema: schema, refCount: 1}
}

type TransformingRecordReader struct {
	refCount int64

	rdr          array.RecordReader
	transformer  RecordTransformer
	targetSchema *arrow.Schema

	cur arrow.RecordBatch
	err error
}

// Record implements array.RecordReader.
func (r *TransformingRecordReader) Record() arrow.RecordBatch {
	return r.RecordBatch()
}

// RecordBatch implements array.RecordReader.
func (r *TransformingRecordReader) RecordBatch() arrow.RecordBatch {
	return r.cur
}

func (r *TransformingRecordReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *TransformingRecordReader) Release() {
	if atomic.LoadInt64(&r.refCount) < 0 {
		panic("too many releases")
	}

	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.cur != nil {
			r.cur.Release()
		}
		r.rdr.Release()
	}
}

func (r *TransformingRecordReader) Schema() *arrow.Schema {
	return r.targetSchema
}

func (r *TransformingRecordReader) Next() bool {
	if !r.rdr.Next() {
		return false
	}
	if r.cur != nil {
		r.cur.Release()
	}

	rec := r.rdr.RecordBatch()
	rec.Retain()
	defer rec.Release()

	r.cur = r.transformer(rec)

	return true
}

func (r *TransformingRecordReader) Err() error {
	return errors.Join(r.rdr.Err(), r.err)
}

var _ array.RecordReader = (*TransformingRecordReader)(nil)
