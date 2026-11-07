package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/storage/s3"
	"github.com/google/uuid"
)

const aggregateEventBatchID = "00000000-0000-0000-0000-000000000000"

func (s *Service) StartAggregation(ctx context.Context, runID string) error {
	const op = "Service.StartAggregation"

	run, err := s.repo.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("%s: get run: %w", op, err)
	}
	if run.Status != models.RunStatusAggregating {
		return nil
	}

	agg, err := s.loadAggregation(ctx, run)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if agg.Status != models.BatchStatusPending {
		return nil
	}

	batchesURI := agg.BatchesURI
	if batchesURI == "" {
		batchesURI, err = s.writeManifest(ctx, run)
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
	}

	if err := s.dispatchAggregation(ctx, runID, batchesURI); err != nil {
		return fmt.Errorf("%s: dispatch: %w", op, err)
	}

	return nil
}

func (s *Service) loadAggregation(ctx context.Context, run *models.Run) (*models.Aggregation, error) {
	agg, err := s.repo.GetAggregation(ctx, run.RunID)
	if err == nil {
		return agg, nil
	}
	if !errors.Is(err, models.ErrAggregationNotFound) {
		return nil, fmt.Errorf("get aggregation: %w", err)
	}

	if err := s.repo.EnsureAggregation(ctx, run.RunID, run.PortfolioID); err != nil {
		return nil, fmt.Errorf("ensure aggregation: %w", err)
	}

	agg, err = s.repo.GetAggregation(ctx, run.RunID)
	if err != nil {
		return nil, fmt.Errorf("get aggregation: %w", err)
	}

	return agg, nil
}

func (s *Service) writeManifest(ctx context.Context, run *models.Run) (string, error) {
	outputs, err := s.repo.ListBatchOutputs(ctx, run.RunID)
	if err != nil {
		return "", fmt.Errorf("list batch outputs: %w", err)
	}

	manifest := models.BatchesManifest{
		RunID:        run.RunID,
		PortfolioID:  run.PortfolioID,
		ReportDttm:   run.ReportDttm.Format(reportDateFmt),
		TotalBatches: len(outputs),
		Batches:      make([]models.ManifestBatch, 0, len(outputs)),
	}
	for _, o := range outputs {
		manifest.Batches = append(manifest.Batches, models.ManifestBatch{
			BatchID:   o.BatchID,
			DealCount: o.DealCount,
			Cashflow:  nonNilLinks(o.Cashflow),
			PV:        nonNilLinks(o.PV),
		})
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("marshal manifest: %w", err)
	}

	key := s3.BatchesKey(run.RunID)
	if err := s.store.Put(ctx, s.cfg.LakeBucket, key, data); err != nil {
		return "", fmt.Errorf("write manifest: %w", err)
	}

	return s3.URI(s.cfg.LakeBucket, key), nil
}

func (s *Service) dispatchAggregation(ctx context.Context, runID, batchesURI string) error {
	return s.repo.WithinTx(ctx, func(ctx context.Context) error {
		run, err := s.repo.LockRun(ctx, runID)
		if err != nil {
			return err
		}
		if run.Status != models.RunStatusAggregating {
			return nil
		}

		agg, err := s.repo.GetAggregation(ctx, runID)
		if err != nil {
			return err
		}
		if agg.Status != models.BatchStatusPending {
			return nil
		}

		attempt := agg.Attempt + 1
		if err := s.repo.MarkAggregationDispatched(ctx, runID, attempt, s.leaseFor(attempt), batchesURI); err != nil {
			return err
		}

		task := events.AggregateTask{
			RunID:        runID,
			PortfolioID:  run.PortfolioID,
			Attempt:      attempt,
			BatchesLink:  batchesURI,
			OutputPrefix: s3.URI(s.cfg.ReportBucket, s3.ReportPrefix(runID)),
		}
		subject := s.cfg.TaskSubjects[models.StageAggregate]
		if err := s.pub.PublishTask(ctx, subject, aggregateMsgID(runID, attempt), task); err != nil {
			return err
		}

		s.log.Info("aggregation dispatched", slog.String("run_id", runID), slog.Int("attempt", attempt))

		return nil
	})
}

func (s *Service) HandleAggregateEvent(ctx context.Context, event events.AggregateEvent) error {
	const op = "Service.HandleAggregateEvent"

	if _, err := uuid.Parse(event.RunID); err != nil || event.Attempt < 1 {
		s.log.Warn("invalid aggregate event", slog.String("run_id", event.RunID), slog.Int("attempt", event.Attempt))
		return nil
	}

	err := s.repo.WithinTx(ctx, func(ctx context.Context) error {
		return s.applyAggregateEvent(ctx, event)
	})
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (s *Service) applyAggregateEvent(ctx context.Context, event events.AggregateEvent) error {
	log := s.log.With(
		slog.String("run_id", event.RunID),
		slog.String("stage", string(models.StageAggregate)),
		slog.Int("attempt", event.Attempt),
	)

	run, err := s.repo.LockRun(ctx, event.RunID)
	if isNotFound(err) {
		log.Warn("event for unknown run")
		return nil
	}
	if err != nil {
		return err
	}

	if !strings.EqualFold(run.PortfolioID, event.PortfolioID) {
		log.Warn("aggregate event portfolio mismatch ignored",
			slog.String("portfolio_id", event.PortfolioID),
			slog.String("expected_portfolio_id", run.PortfolioID),
		)
		return nil
	}

	fresh, err := s.repo.MarkEventProcessed(ctx, event.RunID, aggregateEventBatchID, models.StageAggregate, event.Attempt)
	if err != nil {
		return err
	}
	if !fresh {
		log.Debug("duplicate event ignored")
		return nil
	}

	if run.Status != models.RunStatusAggregating {
		log.Debug("event for inactive run ignored", slog.String("status", string(run.Status)))
		return nil
	}

	agg, err := s.repo.GetAggregation(ctx, event.RunID)
	if isNotFound(err) {
		log.Warn("event for unknown aggregation")
		return nil
	}
	if err != nil {
		return err
	}

	if agg.Status != models.BatchStatusDispatched || agg.Attempt != event.Attempt {
		log.Warn("stale event ignored",
			slog.String("aggregation_status", string(agg.Status)),
			slog.Int("current_attempt", agg.Attempt),
		)
		return nil
	}

	return s.applyAggregateOutcome(ctx, log, agg, event)
}

func (s *Service) applyAggregateOutcome(
	ctx context.Context,
	log *slog.Logger,
	agg *models.Aggregation,
	event events.AggregateEvent,
) error {
	switch {
	case event.Status == events.StatusSuccess && len(event.MissingBatches) == 0:
		if err := s.repo.MarkAggregationDone(ctx, agg.RunID, event.ReportLink, event.Stats); err != nil {
			return err
		}
		if err := s.repo.CompleteRun(ctx, agg.RunID, event.ReportLink); err != nil {
			return err
		}
		log.Info("run succeeded", slog.String("report_link", event.ReportLink))
		return nil

	case event.Status == events.StatusSuccess:
		reason := "aggregator reported missing batches: " + strings.Join(event.MissingBatches, ", ")
		log.Error("aggregator reported missing batches", slog.Any("missing_batches", event.MissingBatches))
		if err := s.repo.MarkAggregationDead(ctx, agg.RunID, reason); err != nil {
			return err
		}
		return s.repo.FailRun(ctx, agg.RunID, reason)

	case event.Status == events.StatusError:
		_, err := s.retryOrKillAggregation(ctx, agg, reasonWorkerError)
		return err

	default:
		log.Warn("event with unknown status ignored", slog.String("status", event.Status))
		return nil
	}
}

func (s *Service) retryOrKillAggregation(ctx context.Context, agg *models.Aggregation, reason string) (bool, error) {
	log := s.log.With(
		slog.String("run_id", agg.RunID),
		slog.String("stage", string(models.StageAggregate)),
		slog.Int("attempt", agg.Attempt),
	)
	reason = reason + " on attempt " + strconv.Itoa(agg.Attempt)

	if agg.Attempt < s.cfg.MaxAttempts {
		if err := s.repo.MarkAggregationRetry(ctx, agg.RunID, reason); err != nil {
			return false, err
		}
		log.Warn("aggregation will be retried", slog.String("reason", reason))
		return false, nil
	}

	if err := s.repo.MarkAggregationDead(ctx, agg.RunID, reason); err != nil {
		return false, err
	}

	runErr := fmt.Sprintf("aggregation is dead after %d attempts: %s", agg.Attempt, reason)
	if err := s.repo.FailRun(ctx, agg.RunID, runErr); err != nil {
		return false, err
	}

	log.Error("run failed", slog.String("reason", runErr))

	return true, nil
}

func aggregateMsgID(runID string, attempt int) string {
	return runID + msgIDSep + string(models.StageAggregate) + msgIDSep + strconv.Itoa(attempt)
}

func nonNilLinks(links []models.OutputLink) []models.OutputLink {
	if links == nil {
		return []models.OutputLink{}
	}

	return links
}
