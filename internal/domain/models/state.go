package models

import (
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("snapshot already registered with different parameters")
	ErrInvalid  = errors.New("invalid input")
)

type State string

const (
	Waiting     State = "waiting"
	Preparing   State = "preparing"
	Running     State = "running"
	Aggregating State = "aggregating"
	Succeeded   State = "succeeded"
	Failed      State = "failed"
	Canceled    State = "canceled"
)

func (s State) Terminal() bool { return s == Succeeded || s == Failed || s == Canceled }

type Stage string

const (
	Cashflow  Stage = "cashflow"
	PV        Stage = "pv"
	Aggregate Stage = "aggregate"
	Done      Stage = "done"
)

type Run struct {
	ID               string    `json:"run_id"`
	SnapshotID       string    `json:"snapshot_id"`
	PortfolioID      string    `json:"portfolio_id,omitempty"`
	ReportDate       string    `json:"report_dttm,omitempty"`
	CurvesURI        string    `json:"curves_uri,omitempty"`
	PreparedURI      string    `json:"prepared_uri,omitempty"`
	SourceVersion    string    `json:"source_version_id,omitempty"`
	SourceETag       string    `json:"source_etag,omitempty"`
	EngineVersion    string    `json:"engine_version,omitempty"`
	DefaultedFields  []string  `json:"defaulted_fields,omitempty"`
	Ready            bool      `json:"prepared"`
	State            State     `json:"state"`
	TotalDeals       int       `json:"total_deals"`
	TotalBatches     int       `json:"total_batches"`
	TaskID           string    `json:"task_id,omitempty"`
	PlanningToken    string    `json:"planning_token,omitempty"`
	PlanningAttempts int       `json:"planning_attempts,omitempty"`
	PlanningUntil    time.Time `json:"planning_until,omitempty"`
	ReportLink       string    `json:"report_link,omitempty"`
	Error            string    `json:"error,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}
type Batch struct {
	ID            string       `json:"batch_id"`
	RunID         string       `json:"run_id"`
	Seq           int          `json:"seq"`
	DealCount     int          `json:"deal_count"`
	IDsURI        string       `json:"deal_ids_link"`
	Stage         Stage        `json:"stage"`
	TaskID        string       `json:"task_id"`
	CashflowLinks []OutputLink `json:"cashflow_links,omitempty"`
	PVLinks       []OutputLink `json:"pv_links,omitempty"`
	Error         string       `json:"error,omitempty"`
}
type OutputLink struct {
	URI string `json:"uri"`
}

type Task struct {
	ID            string       `json:"task_id"`
	RunID         string       `json:"run_id"`
	BatchID       string       `json:"batch_id,omitempty"`
	SnapshotID    string       `json:"snapshot_id"`
	Stage         Stage        `json:"stage"`
	Attempt       int          `json:"attempt"`
	PortfolioID   string       `json:"portfolio_id"`
	ReportDate    string       `json:"report_dttm"`
	SourceURI     string       `json:"source_uri"`
	SourceVersion string       `json:"source_version_id,omitempty"`
	SourceETag    string       `json:"source_etag,omitempty"`
	CurvesURI     string       `json:"curves_uri"`
	EngineVersion string       `json:"engine_version,omitempty"`
	IDsURI        string       `json:"deal_ids_link,omitempty"`
	InputLinks    []OutputLink `json:"input_links,omitempty"`
	BatchesURI    string       `json:"batches_link,omitempty"`
	OutputPrefix  string       `json:"output_prefix"`
}
type Delivery struct {
	Task       Task
	State      string
	LeaseUntil time.Time
}
