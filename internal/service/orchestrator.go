package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/storage/s3"
	"github.com/google/uuid"
)

const (
	reportDateFmt = "2006-01-02"
	msgIDSep      = ":"
)

type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return e.Field + ": " + e.Reason
}

type CreateRunInput struct {
	PortfolioID string
	ReportDttm  string
	SourceURI   string
	CurvesURI   string
}

func (s *Service) CreateRun(ctx context.Context, in CreateRunInput) (*models.Run, error) {
	const op = "Service.CreateRun"

	run, err := newRun(in)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	if err := s.repo.CreateRun(ctx, run); err != nil {
		return nil, fmt.Errorf("%s: create run: %w", op, err)
	}

	s.log.Info("run created",
		slog.String("run_id", run.RunID),
		slog.String("portfolio_id", run.PortfolioID),
		slog.String("source_uri", run.SourceURI),
	)

	s.enqueue(run.RunID)

	return run, nil
}

func newRun(in CreateRunInput) (*models.Run, error) {
	if _, err := uuid.Parse(in.PortfolioID); err != nil {
		return nil, &ValidationError{Field: "portfolio_id", Reason: "must be a UUID"}
	}

	reportDttm, err := time.Parse(reportDateFmt, in.ReportDttm)
	if err != nil {
		return nil, &ValidationError{Field: "report_dttm", Reason: "must be a date in YYYY-MM-DD format"}
	}

	if _, _, err := s3.ParseURI(in.SourceURI); err != nil || strings.HasSuffix(in.SourceURI, "/") {
		return nil, &ValidationError{Field: "source_uri", Reason: "must be an s3://bucket/key object uri"}
	}

	curvesURI := in.CurvesURI
	if _, _, err := s3.ParseURI(curvesURI); err != nil {
		return nil, &ValidationError{Field: "curves_uri", Reason: "must be an s3://bucket/prefix/ uri"}
	}
	if !strings.HasSuffix(curvesURI, "/") {
		curvesURI += "/"
	}

	return &models.Run{
		RunID:       uuid.NewString(),
		PortfolioID: strings.ToLower(in.PortfolioID),
		ReportDttm:  reportDttm,
		SourceURI:   in.SourceURI,
		CurvesURI:   curvesURI,
		Status:      models.RunStatusCreated,
	}, nil
}

func (s *Service) GetRun(ctx context.Context, runID string) (*models.RunDetails, error) {
	const op = "Service.GetRun"

	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, models.ErrRunNotFound)
	}

	run, err := s.repo.GetRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("%s: get run: %w", op, err)
	}

	progress, err := s.repo.ListStageProgress(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("%s: list progress: %w", op, err)
	}

	return &models.RunDetails{Run: run, Stages: progress}, nil
}

func (s *Service) ListRunBatches(ctx context.Context, runID string, filter models.BatchStageFilter) ([]*models.BatchDetails, error) {
	const op = "Service.ListRunBatches"

	if filter.Stage != "" && !filter.Stage.IsBatchStage() {
		return nil, fmt.Errorf("%s: %w", op, &ValidationError{Field: "stage", Reason: "must be one of prep, cashflow, pv"})
	}
	if filter.Status != "" && !filter.Status.IsValid() {
		return nil, fmt.Errorf("%s: %w", op, &ValidationError{Field: "status", Reason: "unknown batch status"})
	}
	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, models.ErrRunNotFound)
	}

	if _, err := s.repo.GetRun(ctx, runID); err != nil {
		return nil, fmt.Errorf("%s: get run: %w", op, err)
	}

	batches, err := s.repo.ListBatches(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("%s: list batches: %w", op, err)
	}

	stages, err := s.repo.ListBatchStages(ctx, runID, filter)
	if err != nil {
		return nil, fmt.Errorf("%s: list batch stages: %w", op, err)
	}

	return groupBatchStages(batches, stages), nil
}

func groupBatchStages(batches []*models.Batch, stages []*models.BatchStage) []*models.BatchDetails {
	byBatch := make(map[string][]*models.BatchStage, len(batches))
	for _, bs := range stages {
		byBatch[bs.BatchID] = append(byBatch[bs.BatchID], bs)
	}

	details := make([]*models.BatchDetails, 0, len(batches))
	for _, b := range batches {
		rows, ok := byBatch[b.BatchID]
		if !ok {
			continue
		}
		details = append(details, &models.BatchDetails{Batch: b, Stages: rows})
	}

	return details
}

func (s *Service) CancelRun(ctx context.Context, runID string) (*models.RunDetails, error) {
	const op = "Service.CancelRun"

	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, models.ErrRunNotFound)
	}

	err := s.repo.WithinTx(ctx, func(ctx context.Context) error {
		run, err := s.repo.LockRun(ctx, runID)
		if err != nil {
			return err
		}
		if run.Status.IsTerminal() {
			return models.ErrRunFinished
		}
		return s.repo.CancelRun(ctx, runID)
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	s.log.Info("run canceled", slog.String("run_id", runID))

	return s.GetRun(ctx, runID)
}

func (s *Service) processRun(ctx context.Context, runID string) {
	run, err := s.repo.GetRun(ctx, runID)
	if err != nil {
		s.log.Error("load run failed", slog.String("run_id", runID), slog.Any("error", err))
		return
	}

	if err := s.prepareRun(ctx, run); err != nil {
		if ctx.Err() != nil {
			s.log.Warn("run processing interrupted", slog.String("run_id", runID), slog.Any("error", err))
			return
		}
		s.failRun(ctx, runID, err)
		return
	}

	if err := s.DispatchStage(ctx, runID, models.StagePrep); err != nil {
		s.log.Error("dispatch failed, scanner will retry",
			slog.String("run_id", runID),
			slog.String("stage", string(models.StagePrep)),
			slog.Any("error", err),
		)
	}
}

func (s *Service) prepareRun(ctx context.Context, run *models.Run) error {
	switch run.Status {
	case models.RunStatusCreated:
		if err := s.land(ctx, run); err != nil {
			return err
		}
		if err := s.repo.UpdateRunStatus(ctx, run.RunID, models.RunStatusSplitting); err != nil {
			return fmt.Errorf("set splitting: %w", err)
		}
		return s.splitRun(ctx, run)
	case models.RunStatusSplitting:
		return s.splitRun(ctx, run)
	default:
		return nil
	}
}

func (s *Service) land(ctx context.Context, run *models.Run) error {
	srcBucket, srcKey, err := s3.ParseURI(run.SourceURI)
	if err != nil {
		return fmt.Errorf("parse source uri: %w", err)
	}

	dealsKey := s3.DealsKey(run.RunID)
	if srcBucket != s.cfg.LakeBucket || srcKey != dealsKey {
		if err := s.store.Copy(ctx, srcBucket, srcKey, s.cfg.LakeBucket, dealsKey); err != nil {
			return fmt.Errorf("land deals: %w", err)
		}
	}

	curvesBucket, curvesPrefix, err := s3.ParseURI(run.CurvesURI)
	if err != nil {
		return fmt.Errorf("parse curves uri: %w", err)
	}

	dstCurves := s3.CurvesPrefix(run.RunID)
	if curvesBucket == s.cfg.LakeBucket && curvesPrefix == dstCurves {
		return nil
	}

	copied, err := s.store.CopyPrefix(ctx, curvesBucket, curvesPrefix, s.cfg.LakeBucket, dstCurves)
	if err != nil {
		return fmt.Errorf("land curves: %w", err)
	}
	if copied == 0 {
		s.log.Warn("no curve files found", slog.String("run_id", run.RunID), slog.String("curves_uri", run.CurvesURI))
	}

	s.log.Info("run input landed", slog.String("run_id", run.RunID), slog.Int("curve_files", copied))

	return nil
}

func (s *Service) splitRun(ctx context.Context, run *models.Run) error {
	rc, err := s.store.Open(ctx, s.cfg.LakeBucket, s3.DealsKey(run.RunID))
	if err != nil {
		return fmt.Errorf("open deals: %w", err)
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			s.log.Warn("deals reader close failed", slog.String("run_id", run.RunID), slog.Any("error", closeErr))
		}
	}()

	result, err := s.splitter.Split(ctx, run.RunID, rc)
	if err != nil {
		return err
	}

	contextURI, err := s.writeContext(ctx, run, result)
	if err != nil {
		return err
	}

	if err := s.repo.StartRunStages(ctx, run.RunID, contextURI, result.TotalDeals, result.TotalBatches); err != nil {
		return fmt.Errorf("start stages: %w", err)
	}

	s.log.Info("run split",
		slog.String("run_id", run.RunID),
		slog.Int("deals", result.TotalDeals),
		slog.Int("batches", result.TotalBatches),
	)

	return nil
}

func (s *Service) writeContext(ctx context.Context, run *models.Run, result SplitResult) (string, error) {
	lake := s.cfg.LakeBucket

	rc := models.RunContext{
		LayoutVersion:  models.LayoutVersion,
		RunID:          run.RunID,
		PortfolioID:    run.PortfolioID,
		ReportDttm:     run.ReportDttm.Format(reportDateFmt),
		SourceURI:      s3.URI(lake, s3.DealsKey(run.RunID)),
		CurvesURI:      s3.URI(lake, s3.CurvesPrefix(run.RunID)),
		PreparedPrefix: s3.URI(lake, s3.StageRunPrefix(models.StagePrep, run.RunID)),
		CashflowPrefix: s3.URI(lake, s3.StageRunPrefix(models.StageCashflow, run.RunID)),
		PVPrefix:       s3.URI(lake, s3.StageRunPrefix(models.StagePV, run.RunID)),
		TotalDeals:     result.TotalDeals,
		TotalBatches:   result.TotalBatches,
		CreatedAt:      run.CreatedAt.UTC().Format(time.RFC3339),
	}

	data, err := json.Marshal(rc)
	if err != nil {
		return "", fmt.Errorf("marshal context: %w", err)
	}

	key := s3.ContextKey(run.RunID)
	if err := s.store.Put(ctx, lake, key, data); err != nil {
		return "", fmt.Errorf("write context: %w", err)
	}

	return s3.URI(lake, key), nil
}

func (s *Service) DispatchStage(ctx context.Context, runID string, stage models.Stage) error {
	const op = "Service.DispatchStage"

	ids, err := s.repo.ListPendingBatchIDs(ctx, runID, stage)
	if err != nil {
		return fmt.Errorf("%s: list pending batches: %w", op, err)
	}

	dispatched := 0
	for _, batchID := range ids {
		ok, err := s.dispatchBatch(ctx, runID, batchID, stage)
		if err != nil {
			return fmt.Errorf("%s: dispatch batch %s: %w", op, batchID, err)
		}
		if ok {
			dispatched++
		}
	}

	if dispatched > 0 {
		s.log.Info("stage dispatched",
			slog.String("run_id", runID),
			slog.String("stage", string(stage)),
			slog.Int("batches", dispatched),
		)
	}

	return nil
}

func (s *Service) dispatchBatch(ctx context.Context, runID, batchID string, stage models.Stage) (bool, error) {
	dispatched := false

	err := s.repo.WithinTx(ctx, func(ctx context.Context) error {
		run, err := s.repo.LockRun(ctx, runID)
		if err != nil {
			return err
		}
		if run.Status != models.RunStatusRunning || run.CurrentStage != stage {
			return nil
		}

		bs, err := s.repo.GetBatchStage(ctx, runID, batchID, stage)
		if err != nil {
			return err
		}
		if bs.Status != models.BatchStatusPending {
			return nil
		}

		attempt := bs.Attempt + 1
		if err := s.repo.MarkBatchDispatched(ctx, runID, batchID, stage, attempt, s.leaseFor(attempt)); err != nil {
			return err
		}

		task := events.BatchTask{
			RunID:        runID,
			BatchID:      batchID,
			Attempt:      attempt,
			DealIDsLink:  s3.URI(s.cfg.LakeBucket, s3.IdsKey(runID, batchID)),
			OutputPrefix: s3.URI(s.cfg.LakeBucket, s3.StagePrefix(stage, runID, batchID)),
		}
		if stage == models.StageCashflow || stage == models.StagePV {
			task.EngineVersion = events.DefaultEngineVersion
		}
		if err := s.pub.PublishTask(ctx, s.cfg.TaskSubjects[stage], batchMsgID(runID, batchID, stage, attempt), task); err != nil {
			return err
		}

		s.log.Debug("batch dispatched",
			slog.String("run_id", runID),
			slog.String("batch_id", batchID),
			slog.String("stage", string(stage)),
			slog.Int("attempt", attempt),
		)
		dispatched = true

		return nil
	})

	return dispatched, err
}

func (s *Service) Recover(ctx context.Context) error {
	const op = "Service.Recover"

	runs, err := s.repo.ListActiveRuns(ctx)
	if err != nil {
		return fmt.Errorf("%s: list active runs: %w", op, err)
	}

	for _, run := range runs {
		if ctx.Err() != nil {
			return fmt.Errorf("%s: %w", op, ctx.Err())
		}

		if err := s.resumeRun(ctx, run); err != nil {
			s.failRun(ctx, run.RunID, fmt.Errorf("recovery: %w", err))
			continue
		}

		s.log.Info("run recovered",
			slog.String("run_id", run.RunID),
			slog.String("status", string(run.Status)),
			slog.String("stage", string(run.CurrentStage)),
		)
	}

	return nil
}

func (s *Service) resumeRun(ctx context.Context, run *models.Run) error {
	switch run.Status {
	case models.RunStatusCreated, models.RunStatusSplitting:
		s.enqueue(run.RunID)
		return nil
	case models.RunStatusRunning:
		return s.DispatchStage(ctx, run.RunID, run.CurrentStage)
	case models.RunStatusAggregating:
		return s.StartAggregation(ctx, run.RunID)
	default:
		return nil
	}
}

func (s *Service) failRun(ctx context.Context, runID string, cause error) {
	s.log.Error("run failed", slog.String("run_id", runID), slog.Any("error", cause))

	if err := s.repo.FailRun(ctx, runID, cause.Error()); err != nil {
		s.log.Error("mark run failed", slog.String("run_id", runID), slog.Any("error", err))
	}
}

func batchMsgID(runID, batchID string, stage models.Stage, attempt int) string {
	return runID + msgIDSep + batchID + msgIDSep + string(stage) + msgIDSep + strconv.Itoa(attempt)
}

func isNotFound(err error) bool {
	return errors.Is(err, models.ErrRunNotFound) ||
		errors.Is(err, models.ErrBatchNotFound) ||
		errors.Is(err, models.ErrAggregationNotFound)
}
