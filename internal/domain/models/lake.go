package models

const LayoutVersion = 1

type RunContext struct {
	LayoutVersion  int    `json:"layout_version"`
	RunID          string `json:"run_id"`
	PortfolioID    string `json:"portfolio_id"`
	ReportDttm     string `json:"report_dttm"`
	SourceURI      string `json:"source_uri"`
	CurvesURI      string `json:"curves_uri"`
	PreparedPrefix string `json:"prepared_prefix"`
	CashflowPrefix string `json:"cashflow_prefix"`
	PVPrefix       string `json:"pv_prefix"`
	TotalDeals     int    `json:"total_deals"`
	TotalBatches   int    `json:"total_batches"`
	CreatedAt      string `json:"created_at"`
}

type BatchesManifest struct {
	RunID        string          `json:"run_id"`
	PortfolioID  string          `json:"portfolio_id"`
	ReportDttm   string          `json:"report_dttm"`
	TotalBatches int             `json:"total_batches"`
	Batches      []ManifestBatch `json:"batches"`
}

type ManifestBatch struct {
	BatchID   string       `json:"batch_id"`
	DealCount int          `json:"deal_count"`
	Cashflow  []OutputLink `json:"cashflow"`
	PV        []OutputLink `json:"pv"`
}
