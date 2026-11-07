package service

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/google/uuid"
)

const (
	statDealsIn = "deals_in"
	statDealsOK = "deals_ok"

	reasonWorkerError  = "worker reported error"
	reasonLeaseExpired = "lease expired"
)

type followUp struct {
	dispatch  models.Stage
	aggregate bool
}

func (s *Service) HandleBatchEvent(ctx context.Context, stage models.Stage, event events.BatchEvent) error {
	const op = "Service.HandleBatchEvent"

	if !stage.IsBatchStage() {
		return fmt.Errorf("%s: stage %s is not a batch stage", op, stage)
	}
	if !validBatchEvent(event) {
		s.log.Warn("invalid batch event",
			slog.String("run_id", event.RunID),
			slog.String("batch_id", event.BatchID),
			slog.String("stage", string(stage)),
			slog.Int("attempt", event.Attempt),
		)
		return nil
	}

	var next followUp
	err := s.repo.WithinTx(ctx, func(ctx context.Context) error {
		var err error
		next, err = s.applyBatchEvent(ctx, stage, event)
		return err
	})
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	s.runFollowUp(event.RunID, next)

	return nil
}

func validBatchEvent(event events.BatchEvent) bool {
	if _, err := uuid.Parse(event.RunID); err != nil {
		return false
	}
	if _, err := uuid.Parse(event.BatchID); err != nil {
		return false
	}

	return event.Attempt >= 1
}

func (s *Service) applyBatchEvent(ctx context.Context, stage models.Stage, event events.BatchEvent) (followUp, error) {
	log := s.log.With(
		slog.String("run_id", event.RunID),
		slog.String("batch_id", event.BatchID),
		slog.String("stage", string(stage)),
		slog.Int("attempt", event.Attempt),
	)

	run, err := s.repo.LockRun(ctx, event.RunID)
	if isNotFound(err) {
		log.Warn("event for unknown run")
		return followUp{}, nil
	}
	if err != nil {
		return followUp{}, err
	}

	fresh, err := s.repo.MarkEventProcessed(ctx, event.RunID, event.BatchID, stage, event.Attempt)
	if err != nil {
		return followUp{}, err
	}
	if !fresh {
		log.Debug("duplicate event ignored")
		return followUp{}, nil
	}

	if run.Status != models.RunStatusRunning {
		log.Debug("event for inactive run ignored", slog.String("status", string(run.Status)))
		return followUp{}, nil
	}

	bs, err := s.repo.GetBatchStage(ctx, event.RunID, event.BatchID, stage)
	if isNotFound(err) {
		log.Warn("event for unknown batch")
		return followUp{}, nil
	}
	if err != nil {
		return followUp{}, err
	}

	if bs.Status != models.BatchStatusDispatched || bs.Attempt != event.Attempt {
		log.Warn("stale event ignored",
			slog.String("batch_status", string(bs.Status)),
			slog.Int("current_attempt", bs.Attempt),
		)
		return followUp{}, nil
	}

	return s.applyBatchOutcome(ctx, log, run, bs, event)
}

func (s *Service) applyBatchOutcome(
	ctx context.Context,
	log *slog.Logger,
	run *models.Run,
	bs *models.BatchStage,
	event events.BatchEvent,
) (followUp, error) {
	switch event.Status {
	case events.StatusSuccess:
		if err := s.repo.MarkBatchDone(ctx, bs.RunID, bs.BatchID, bs.Stage, event.OutputLinks, event.Stats); err != nil {
			return followUp{}, err
		}
		s.checkDealsChain(ctx, log, bs, event.Stats)
	case events.StatusError:
		_, err := s.retryOrKillBatch(ctx, bs, reasonWorkerError)
		return followUp{}, err
	default:
		log.Warn("event with unknown status ignored", slog.String("status", event.Status))
		return followUp{}, nil
	}

	return s.closeStageIfDone(ctx, run, bs.Stage)
}

func (s *Service) retryOrKillBatch(ctx context.Context, bs *models.BatchStage, reason string) (bool, error) {
	log := s.log.With(
		slog.String("run_id", bs.RunID),
		slog.String("batch_id", bs.BatchID),
		slog.String("stage", string(bs.Stage)),
		slog.Int("attempt", bs.Attempt),
	)
	reason = reason + " on attempt " + strconv.Itoa(bs.Attempt)

	if bs.Attempt < s.cfg.MaxAttempts {
		if err := s.repo.MarkBatchRetry(ctx, bs.RunID, bs.BatchID, bs.Stage, reason); err != nil {
			return false, err
		}
		log.Warn("batch will be retried", slog.String("reason", reason))
		return false, nil
	}

	if err := s.repo.MarkBatchDead(ctx, bs.RunID, bs.BatchID, bs.Stage, reason); err != nil {
		return false, err
	}

	runErr := fmt.Sprintf("batch %s is dead at stage %s after %d attempts: %s",
		bs.BatchID, bs.Stage, bs.Attempt, reason)
	if err := s.repo.FailRun(ctx, bs.RunID, runErr); err != nil {
		return false, err
	}

	log.Error("run failed", slog.String("reason", runErr))

	return true, nil
}

func (s *Service) closeStageIfDone(ctx context.Context, run *models.Run, stage models.Stage) (followUp, error) {
	const op = "Service.closeStageIfDone"

	if run.CurrentStage != stage {
		return followUp{}, nil
	}

	stages, err := s.repo.ListStageProgress(ctx, run.RunID, stage)
	if err != nil {
		return followUp{}, err
	}
	if len(stages) == 0 {
		return followUp{}, fmt.Errorf("%s: no progress for stage %s", op, stage)
	}
	progress := stages[0]
	if !progress.IsClosed() {
		return followUp{}, nil
	}

	next, _ := stage.Next()

	s.log.Info("stage closed",
		slog.String("run_id", run.RunID),
		slog.String("stage", string(stage)),
		slog.String("next_stage", string(next)),
		slog.Int("batches", progress.Total),
	)

	if next.IsBatchStage() {
		if err := s.repo.UpdateRunStage(ctx, run.RunID, models.RunStatusRunning, next); err != nil {
			return followUp{}, err
		}
		return followUp{dispatch: next}, nil
	}

	if err := s.repo.UpdateRunStage(ctx, run.RunID, models.RunStatusAggregating, models.StageAggregate); err != nil {
		return followUp{}, err
	}
	if err := s.repo.EnsureAggregation(ctx, run.RunID, run.PortfolioID); err != nil {
		return followUp{}, err
	}

	return followUp{aggregate: true}, nil
}

func (s *Service) checkDealsChain(ctx context.Context, log *slog.Logger, bs *models.BatchStage, stats map[string]int64) {
	dealsIn, ok := stats[statDealsIn]
	if !ok {
		return
	}

	expected, ok := s.expectedDealsIn(ctx, log, bs)
	if !ok || expected == dealsIn {
		return
	}

	log.Warn("deals_in does not match previous stage",
		slog.Int64("deals_in", dealsIn),
		slog.Int64("expected", expected),
	)
}

func (s *Service) expectedDealsIn(ctx context.Context, log *slog.Logger, bs *models.BatchStage) (int64, bool) {
	prev, ok := bs.Stage.Prev()
	if !ok {
		return int64(bs.DealCount), true
	}

	prevStage, err := s.repo.GetBatchStage(ctx, bs.RunID, bs.BatchID, prev)
	if err != nil {
		log.Warn("load previous stage for deals check failed", slog.Any("error", err))
		return 0, false
	}

	if v, ok := prevStage.Stats[statDealsOK]; ok {
		return v, true
	}
	if v, ok := prevStage.Stats[statDealsIn]; ok {
		return v, true
	}

	return 0, false
}

func (s *Service) runFollowUp(runID string, next followUp) {
	if next.dispatch != "" || next.aggregate {
		s.enqueue(runID)
	}
}
