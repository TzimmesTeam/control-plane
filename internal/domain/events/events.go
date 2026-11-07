package events

import "github.com/TzimmesTeam/control-plane/internal/domain/models"

const (
	StatusSuccess = "success"
	StatusError   = "error"
)

type BatchEvent struct {
	RunID       string              `json:"run_id"`
	BatchID     string              `json:"batch_id"`
	Attempt     int                 `json:"attempt"`
	Status      string              `json:"status"`
	OutputLinks []models.OutputLink `json:"output_links"`
	Stats       map[string]int64    `json:"stats"`
}

type AggregateEvent struct {
	RunID          string           `json:"run_id"`
	PortfolioID    string           `json:"portfolio_id"`
	Attempt        int              `json:"attempt"`
	Status         string           `json:"status"`
	ReportLink     string           `json:"report_link"`
	MissingBatches []string         `json:"missing_batches"`
	Stats          map[string]int64 `json:"stats"`
}
