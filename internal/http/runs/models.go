package runs

import (
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

const reportDateFmt = "2006-01-02"

type CreateRunRequest struct {
	PortfolioID string `json:"portfolio_id"`
	ReportDttm  string `json:"report_dttm"`
	SourceURI   string `json:"source_uri"`
	CurvesURI   string `json:"curves_uri"`
}

type CreateRunResponse struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type StageProgressResponse struct {
	Stage  string `json:"stage"`
	Total  int    `json:"total"`
	Done   int    `json:"done"`
	Failed int    `json:"failed"`
}

type RunResponse struct {
	RunID        string                  `json:"run_id"`
	PortfolioID  string                  `json:"portfolio_id"`
	ReportDttm   string                  `json:"report_dttm"`
	Status       string                  `json:"status"`
	CurrentStage *string                 `json:"current_stage"`
	TotalDeals   int                     `json:"total_deals"`
	TotalBatches int                     `json:"total_batches"`
	Stages       []StageProgressResponse `json:"stages"`
	ReportLink   *string                 `json:"report_link"`
	Error        *string                 `json:"error"`
	CreatedAt    time.Time               `json:"created_at"`
	FinishedAt   *time.Time              `json:"finished_at"`
}

type OutputLinkResponse struct {
	URI string `json:"uri"`
}

type BatchStageResponse struct {
	Stage       string               `json:"stage"`
	Status      string               `json:"status"`
	Attempt     int                  `json:"attempt"`
	Stats       map[string]int64     `json:"stats,omitempty"`
	OutputLinks []OutputLinkResponse `json:"output_links,omitempty"`
	Error       string               `json:"error,omitempty"`
}

type BatchResponse struct {
	BatchID   string               `json:"batch_id"`
	Seq       int                  `json:"seq"`
	Currency  string               `json:"currency"`
	DealCount int                  `json:"deal_count"`
	Weight    int64                `json:"weight"`
	Stages    []BatchStageResponse `json:"stages"`
}

type BatchesResponse struct {
	Batches []BatchResponse `json:"batches"`
}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

func toRunResponse(details *models.RunDetails) RunResponse {
	run := details.Run

	stages := make([]StageProgressResponse, 0, len(details.Stages))
	for _, p := range details.Stages {
		stages = append(stages, StageProgressResponse{
			Stage:  string(p.Stage),
			Total:  p.Total,
			Done:   p.Done,
			Failed: p.Failed,
		})
	}

	return RunResponse{
		RunID:        run.RunID,
		PortfolioID:  run.PortfolioID,
		ReportDttm:   run.ReportDttm.Format(reportDateFmt),
		Status:       string(run.Status),
		CurrentStage: optional(string(run.CurrentStage)),
		TotalDeals:   run.TotalDeals,
		TotalBatches: run.TotalBatches,
		Stages:       stages,
		ReportLink:   optional(run.ReportLink),
		Error:        optional(run.Error),
		CreatedAt:    run.CreatedAt.UTC(),
		FinishedAt:   utcPtr(run.FinishedAt),
	}
}

func toBatchesResponse(batches []*models.BatchDetails) BatchesResponse {
	resp := BatchesResponse{Batches: make([]BatchResponse, 0, len(batches))}

	for _, d := range batches {
		stages := make([]BatchStageResponse, 0, len(d.Stages))
		for _, bs := range d.Stages {
			stages = append(stages, toBatchStageResponse(bs))
		}

		resp.Batches = append(resp.Batches, BatchResponse{
			BatchID:   d.Batch.BatchID,
			Seq:       d.Batch.Seq,
			Currency:  d.Batch.Currency,
			DealCount: d.Batch.DealCount,
			Weight:    d.Batch.Weight,
			Stages:    stages,
		})
	}

	return resp
}

func toBatchStageResponse(bs *models.BatchStage) BatchStageResponse {
	links := make([]OutputLinkResponse, 0, len(bs.OutputLinks))
	for _, l := range bs.OutputLinks {
		links = append(links, OutputLinkResponse{URI: l.URI})
	}

	return BatchStageResponse{
		Stage:       string(bs.Stage),
		Status:      string(bs.Status),
		Attempt:     bs.Attempt,
		Stats:       bs.Stats,
		OutputLinks: links,
		Error:       bs.Error,
	}
}

func optional(v string) *string {
	if v == "" {
		return nil
	}

	return &v
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}

	v := t.UTC()
	return &v
}
