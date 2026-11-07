package events

const DefaultEngineVersion = "v1"

type BatchTask struct {
	RunID         string `json:"run_id"`
	BatchID       string `json:"batch_id"`
	Attempt       int    `json:"attempt"`
	DealIDsLink   string `json:"deal_ids_link"`
	OutputPrefix  string `json:"output_prefix"`
	EngineVersion string `json:"engine_version,omitempty"`
}

type AggregateTask struct {
	RunID        string `json:"run_id"`
	PortfolioID  string `json:"portfolio_id"`
	Attempt      int    `json:"attempt"`
	BatchesLink  string `json:"batches_link"`
	OutputPrefix string `json:"output_prefix"`
}
