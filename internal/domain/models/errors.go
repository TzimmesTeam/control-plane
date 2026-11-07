package models

import "errors"

var (
	ErrRunNotFound         = errors.New("run not found")
	ErrBatchNotFound       = errors.New("batch not found")
	ErrAggregationNotFound = errors.New("aggregation not found")
	ErrRunFinished         = errors.New("run already finished")
	ErrStateConflict       = errors.New("state changed concurrently")
)
