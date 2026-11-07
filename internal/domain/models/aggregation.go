package models

import "time"

type Aggregation struct {
	RunID        string
	PortfolioID  string
	Status       BatchStatus
	Attempt      int
	BatchesURI   string
	ReportLink   string
	Stats        map[string]int64
	Error        string
	DispatchedAt *time.Time
	DoneAt       *time.Time
	LeaseUntil   *time.Time
}
