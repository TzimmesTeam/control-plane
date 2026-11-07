package events

import (
	"encoding/json"
	"testing"
)

func TestBatchTaskEngineVersionJSON(t *testing.T) {
	prep, err := json.Marshal(BatchTask{RunID: "r", BatchID: "b", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	var prepFields map[string]any
	if err := json.Unmarshal(prep, &prepFields); err != nil {
		t.Fatal(err)
	}
	if _, ok := prepFields["engine_version"]; ok {
		t.Errorf("data-prep task must not carry engine_version: %s", prep)
	}

	calc, err := json.Marshal(BatchTask{RunID: "r", BatchID: "b", Attempt: 1, EngineVersion: DefaultEngineVersion})
	if err != nil {
		t.Fatal(err)
	}
	var calcFields map[string]any
	if err := json.Unmarshal(calc, &calcFields); err != nil {
		t.Fatal(err)
	}
	if calcFields["engine_version"] != DefaultEngineVersion {
		t.Errorf("cashflow/pv task must carry engine_version %q: %s", DefaultEngineVersion, calc)
	}
}
