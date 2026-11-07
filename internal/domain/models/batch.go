package models

import "time"

type BatchStatus string

const (
	BatchStatusPending    BatchStatus = "pending"
	BatchStatusDispatched BatchStatus = "dispatched"
	BatchStatusDone       BatchStatus = "done"
	BatchStatusFailed     BatchStatus = "failed"
	BatchStatusDead       BatchStatus = "dead"
)

func (s BatchStatus) IsValid() bool {
	switch s {
	case BatchStatusPending, BatchStatusDispatched, BatchStatusDone, BatchStatusFailed, BatchStatusDead:
		return true
	default:
		return false
	}
}

type OutputLink struct {
	URI string `json:"uri"`
}

type Batch struct {
	RunID     string
	BatchID   string
	Seq       int
	IdsURI    string
	DealCount int
	Weight    int64
	Currency  string
	CreatedAt time.Time
}

type BatchStage struct {
	RunID        string
	BatchID      string
	Stage        Stage
	Status       BatchStatus
	Attempt      int
	OutputLinks  []OutputLink
	Stats        map[string]int64
	Error        string
	DispatchedAt *time.Time
	DoneAt       *time.Time
	LeaseUntil   *time.Time
	DealCount    int
}

type BatchStageFilter struct {
	Stage  Stage
	Status BatchStatus
}

type BatchDetails struct {
	Batch  *Batch
	Stages []*BatchStage
}

type BatchOutputs struct {
	BatchID   string
	DealCount int
	Cashflow  []OutputLink
	PV        []OutputLink
}
