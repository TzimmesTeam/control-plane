package service

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

func TestPreparedMetadataOverridesDefaults(t *testing.T) {
	s, _, _, pub := fixture(t, 1)
	in := CreateRunInput{SnapshotID: "ab12", PortfolioID: "real-portfolio", ReportDate: "2026-11-17", CurvesURI: "s3://curated/real-curves.parquet"}
	run, err := s.Prepared(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if run.PortfolioID != in.PortfolioID || run.ReportDate != in.ReportDate || run.CurvesURI != in.CurvesURI || len(run.DefaultedFields) != 0 {
		t.Fatal("metadata lost", run)
	}
	s.Tick(context.Background())
	if len(pub.tasks) != 1 || pub.tasks[0].PortfolioID != in.PortfolioID || pub.tasks[0].ReportDate != in.ReportDate || pub.tasks[0].CurvesURI != in.CurvesURI {
		t.Fatal("metadata not forwarded", pub.tasks)
	}
}

func TestPartialMetadataAndConfiguredDefaults(t *testing.T) {
	s, _, _, _ := fixture(t, 1)
	s.cfg.DefaultPortfolioID = "portfolio-{snapshot_id}"
	s.cfg.DefaultReportDate = "2026-10-31"
	s.cfg.DefaultCurvesURI = "s3://curated/configured-curves.parquet"
	run, err := s.Prepared(context.Background(), CreateRunInput{SnapshotID: "ab12", ReportDate: "2026-11-17"})
	if err != nil {
		t.Fatal(err)
	}
	if run.PortfolioID != "portfolio-ab12" || run.ReportDate != "2026-11-17" || run.CurvesURI != s.cfg.DefaultCurvesURI || len(run.DefaultedFields) != 2 || slices.Contains(run.DefaultedFields, "report_dttm") {
		t.Fatal("incorrect fallback", run)
	}
	s.cfg.DefaultReportDate = "2027-01-01"
	s.cfg.DefaultCurvesURI = "s3://curated/new-default.parquet"
	duplicate, err := s.Prepared(context.Background(), CreateRunInput{SnapshotID: "ab12"})
	if err != nil || duplicate.ReportDate != run.ReportDate || duplicate.CurvesURI != run.CurvesURI {
		t.Fatal("duplicate recalculated saved metadata", duplicate, err)
	}
}

func TestCallbackCanReplaceDefaultsBeforePlanning(t *testing.T) {
	s, _, _, _ := fixture(t, 1)
	run, err := s.CreateRun(context.Background(), CreateRunInput{SnapshotID: "ab12"})
	if err != nil || run.Ready || len(run.DefaultedFields) != 3 {
		t.Fatal("optional registration failed", run, err)
	}
	in := CreateRunInput{SnapshotID: "ab12", PortfolioID: "p-real", ReportDate: "2026-11-17", CurvesURI: "s3://curated/real.parquet"}
	updated, err := s.Prepared(context.Background(), in)
	if err != nil || updated.ID != run.ID || !updated.Ready || updated.PortfolioID != in.PortfolioID || updated.ReportDate != in.ReportDate || updated.CurvesURI != in.CurvesURI || len(updated.DefaultedFields) != 0 {
		t.Fatal("callback did not replace fallback", updated, err)
	}
}

func TestStartedRunMetadataIsImmutable(t *testing.T) {
	s, r, _, _ := fixture(t, 1)
	run, err := s.Prepared(context.Background(), CreateRunInput{SnapshotID: "ab12"})
	if err != nil {
		t.Fatal(err)
	}
	s.Tick(context.Background())
	_, err = s.Prepared(context.Background(), CreateRunInput{SnapshotID: "ab12", ReportDate: "2026-11-17"})
	if !errors.Is(err, models.ErrConflict) || r.data.Runs[run.ID].ReportDate != run.ReportDate {
		t.Fatal("running parameters changed", err)
	}
	if _, err := s.Prepared(context.Background(), CreateRunInput{SnapshotID: "ab12"}); err != nil {
		t.Fatal("duplicate callback rejected", err)
	}
}

func TestInvalidOptionalMetadataRejectedWithoutCreatingRun(t *testing.T) {
	for _, in := range []CreateRunInput{
		{SnapshotID: "ab12", ReportDate: "bad-date"},
		{SnapshotID: "ab12", CurvesURI: "https://wrong.example/curves"},
		{SnapshotID: "ab12", PortfolioID: " "},
	} {
		s, r, _, _ := fixture(t, 1)
		if _, err := s.Prepared(context.Background(), in); !errors.Is(err, models.ErrInvalid) {
			t.Fatal("invalid metadata accepted", in, err)
		}
		if len(r.data.Runs) != 0 {
			t.Fatal("invalid callback created a run")
		}
	}
}

func TestSnapshotOnlyCallbackPreservesExplicitRegistration(t *testing.T) {
	s, _, _, _ := fixture(t, 1)
	registered := register(t, s)
	run, err := s.Prepared(context.Background(), CreateRunInput{SnapshotID: "ab12"})
	if err != nil || run.PortfolioID != registered.PortfolioID || run.ReportDate != registered.ReportDate || run.CurvesURI != registered.CurvesURI || len(run.DefaultedFields) != 0 {
		t.Fatal("callback replaced registered parameters", run, err)
	}
}
