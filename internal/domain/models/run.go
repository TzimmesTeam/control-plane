package models

import "time"

type RunStatus string

const (
	RunStatusCreated     RunStatus = "created"
	RunStatusSplitting   RunStatus = "splitting"
	RunStatusRunning     RunStatus = "running"
	RunStatusAggregating RunStatus = "aggregating"
	RunStatusSucceeded   RunStatus = "succeeded"
	RunStatusFailed      RunStatus = "failed"
	RunStatusCanceled    RunStatus = "canceled"
)

func (s RunStatus) IsTerminal() bool {
	return s == RunStatusSucceeded || s == RunStatusFailed || s == RunStatusCanceled
}

type Stage string

const (
	StagePrep      Stage = "prep"
	StageCashflow  Stage = "cashflow"
	StagePV        Stage = "pv"
	StageAggregate Stage = "aggregate"
)

func BatchStages() []Stage {
	return []Stage{StagePrep, StageCashflow, StagePV}
}

func (s Stage) IsBatchStage() bool {
	return s == StagePrep || s == StageCashflow || s == StagePV
}

func (s Stage) Next() (Stage, bool) {
	switch s {
	case StagePrep:
		return StageCashflow, true
	case StageCashflow:
		return StagePV, true
	case StagePV:
		return StageAggregate, true
	default:
		return "", false
	}
}

func (s Stage) Prev() (Stage, bool) {
	switch s {
	case StageCashflow:
		return StagePrep, true
	case StagePV:
		return StageCashflow, true
	case StageAggregate:
		return StagePV, true
	default:
		return "", false
	}
}

type Run struct {
	RunID        string
	PortfolioID  string
	ReportDttm   time.Time
	SourceURI    string
	CurvesURI    string
	ContextURI   string
	Status       RunStatus
	CurrentStage Stage
	TotalDeals   int
	TotalBatches int
	ReportLink   string
	Error        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	FinishedAt   *time.Time
}

type StageProgress struct {
	Stage  Stage
	Total  int
	Done   int
	Failed int
}

func (p StageProgress) IsClosed() bool {
	return p.Total > 0 && p.Done == p.Total
}

type RunDetails struct {
	Run    *Run
	Stages []StageProgress
}
