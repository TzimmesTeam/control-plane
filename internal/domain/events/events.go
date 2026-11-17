package events

import "github.com/TzimmesTeam/control-plane/internal/domain/models"

type Result struct {
	TaskID      string              `json:"task_id"`
	Status      string              `json:"status"`
	OutputLinks []models.OutputLink `json:"output_links,omitempty"`
	ReportLink  string              `json:"report_link,omitempty"`
	Error       string              `json:"error,omitempty"`
}
