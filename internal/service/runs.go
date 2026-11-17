package service

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/storage/s3"
)

var snapshotPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type CreateRunInput struct {
	SnapshotID    string `json:"snapshot_id"`
	PortfolioID   string `json:"portfolio_id,omitempty"`
	ReportDate    string `json:"report_dttm,omitempty"`
	CurvesURI     string `json:"curves_uri,omitempty"`
	PreparedURI   string `json:"prepared_uri,omitempty"`
	EngineVersion string `json:"engine_version,omitempty"`
}

func runID(snapshot string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("control-plane/snapshot/"+snapshot)).String()
}
func validSnapshot(snapshot string) error {
	if !snapshotPattern.MatchString(snapshot) || snapshot == "." || snapshot == ".." {
		return fmt.Errorf("%w: invalid snapshot_id", models.ErrInvalid)
	}
	return nil
}
func (s *Service) CreateRun(ctx context.Context, in CreateRunInput) (*models.Run, error) {
	return s.saveInput(ctx, in, false)
}

// Prepared accepts both the existing snapshot-only callback and optional metadata.
func (s *Service) Prepared(ctx context.Context, in CreateRunInput) (*models.Run, error) {
	return s.saveInput(ctx, in, true)
}

func validateInput(in CreateRunInput) error {
	if err := validSnapshot(in.SnapshotID); err != nil {
		return err
	}
	if in.PortfolioID != "" && (strings.TrimSpace(in.PortfolioID) == "" || len(in.PortfolioID) > 128) {
		return fmt.Errorf("%w: portfolio_id must contain 1 to 128 characters", models.ErrInvalid)
	}
	if in.ReportDate != "" {
		if _, err := time.Parse("2006-01-02", in.ReportDate); err != nil {
			return fmt.Errorf("%w: report_dttm must be YYYY-MM-DD", models.ErrInvalid)
		}
	}
	if in.CurvesURI != "" {
		if _, _, err := s3.ParseURI(in.CurvesURI); err != nil {
			return fmt.Errorf("%w: curves_uri must be an S3 URI", models.ErrInvalid)
		}
	}
	if in.PreparedURI != "" {
		if _, _, err := s3.ParseURI(in.PreparedURI); err != nil || strings.HasSuffix(in.PreparedURI, "/") {
			return fmt.Errorf("%w: prepared_uri must be an S3 object URI", models.ErrInvalid)
		}
	}
	return nil
}

func (s *Service) saveInput(ctx context.Context, in CreateRunInput, ready bool) (*models.Run, error) {
	if err := validateInput(in); err != nil {
		return nil, err
	}
	id := runID(in.SnapshotID)
	var result *models.Run
	err := s.repo.WithinTx(ctx, func(ctx context.Context) error {
		if err := s.repo.EnsureRun(ctx, id, in.SnapshotID); err != nil {
			return err
		}
		run, err := s.repo.LockRun(ctx, id)
		if err != nil {
			return err
		}
		fields := []struct {
			name     string
			provided string
			current  *string
		}{
			{"portfolio_id", in.PortfolioID, &run.PortfolioID},
			{"report_dttm", in.ReportDate, &run.ReportDate},
			{"curves_uri", in.CurvesURI, &run.CurvesURI},
			{"prepared_uri", in.PreparedURI, &run.PreparedURI},
			{"engine_version", in.EngineVersion, &run.EngineVersion},
		}
		for _, field := range fields {
			if field.provided == "" {
				continue
			}
			if *field.current != field.provided && (run.State != models.Waiting || *field.current != "" && !slices.Contains(run.DefaultedFields, field.name)) {
				return fmt.Errorf("%w: %s differs from saved value", models.ErrConflict, field.name)
			}
			if run.State == models.Waiting {
				*field.current = field.provided
				run.DefaultedFields = slices.DeleteFunc(run.DefaultedFields, func(name string) bool { return name == field.name })
			}
		}
		if run.State != models.Waiting {
			result = run
			return nil
		}
		s.applyDefaults(run)
		if err := validateInput(CreateRunInput{SnapshotID: run.SnapshotID, PortfolioID: run.PortfolioID, ReportDate: run.ReportDate, CurvesURI: run.CurvesURI}); err != nil {
			return err
		}
		if ready {
			run.Ready = true
		}
		if err := s.repo.SaveRun(ctx, run); err != nil {
			return err
		}
		result = run
		return nil
	})
	return result, err
}

func (s *Service) applyDefaults(run *models.Run) {
	if run.PortfolioID == "" {
		run.PortfolioID = strings.ReplaceAll(s.cfg.DefaultPortfolioID, "{snapshot_id}", run.SnapshotID)
		run.DefaultedFields = append(run.DefaultedFields, "portfolio_id")
	}
	if run.ReportDate == "" {
		run.ReportDate = s.cfg.DefaultReportDate
		run.DefaultedFields = append(run.DefaultedFields, "report_dttm")
	}
	if run.CurvesURI == "" {
		run.CurvesURI = s.cfg.DefaultCurvesURI
		run.DefaultedFields = append(run.DefaultedFields, "curves_uri")
	}
}
func (s *Service) GetRun(ctx context.Context, id string) (*models.Run, error) {
	return s.repo.GetRun(ctx, id)
}
func (s *Service) ListRunBatches(ctx context.Context, id string) ([]models.Batch, error) {
	if _, err := s.repo.GetRun(ctx, id); err != nil {
		return nil, err
	}
	return s.repo.ListBatches(ctx, id)
}
func (s *Service) CancelRun(ctx context.Context, id string) (*models.Run, error) {
	var result *models.Run
	err := s.repo.WithinTx(ctx, func(ctx context.Context) error {
		run, err := s.repo.LockRun(ctx, id)
		if err != nil {
			return err
		}
		if !run.State.Terminal() {
			run.State = models.Canceled
			if err := s.repo.SaveRun(ctx, run); err != nil {
				return err
			}
		}
		result = run
		return nil
	})
	return result, err
}
