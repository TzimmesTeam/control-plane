package service

import (
	"context"
	"log/slog"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

func (s *Service) scan(ctx context.Context) {
	s.expireBatchLeases(ctx)
	s.expireAggregationLeases(ctx)
	s.resumeActiveRuns(ctx)
}

func (s *Service) expireBatchLeases(ctx context.Context) {
	expired, err := s.repo.ListExpiredBatchStages(ctx, scanBatchSize)
	if err != nil {
		s.log.Error("list expired batch stages failed", slog.Any("error", err))
		return
	}

	for _, bs := range expired {
		if err := s.expireBatchLease(ctx, bs); err != nil {
			s.log.Error("expire batch lease failed",
				slog.String("run_id", bs.RunID),
				slog.String("batch_id", bs.BatchID),
				slog.String("stage", string(bs.Stage)),
				slog.Int("attempt", bs.Attempt),
				slog.Any("error", err),
			)
		}
	}
}

func (s *Service) expireBatchLease(ctx context.Context, expired *models.BatchStage) error {
	return s.repo.WithinTx(ctx, func(ctx context.Context) error {
		run, err := s.repo.LockRun(ctx, expired.RunID)
		if err != nil {
			return err
		}
		if run.Status != models.RunStatusRunning {
			return nil
		}

		bs, err := s.repo.GetBatchStage(ctx, expired.RunID, expired.BatchID, expired.Stage)
		if err != nil {
			return err
		}
		if bs.Status != models.BatchStatusDispatched || bs.Attempt != expired.Attempt {
			return nil
		}

		_, err = s.retryOrKillBatch(ctx, bs, reasonLeaseExpired)
		return err
	})
}

func (s *Service) expireAggregationLeases(ctx context.Context) {
	expired, err := s.repo.ListExpiredAggregations(ctx, scanBatchSize)
	if err != nil {
		s.log.Error("list expired aggregations failed", slog.Any("error", err))
		return
	}

	for _, agg := range expired {
		if err := s.expireAggregationLease(ctx, agg); err != nil {
			s.log.Error("expire aggregation lease failed",
				slog.String("run_id", agg.RunID),
				slog.Int("attempt", agg.Attempt),
				slog.Any("error", err),
			)
		}
	}
}

func (s *Service) expireAggregationLease(ctx context.Context, expired *models.Aggregation) error {
	return s.repo.WithinTx(ctx, func(ctx context.Context) error {
		run, err := s.repo.LockRun(ctx, expired.RunID)
		if err != nil {
			return err
		}
		if run.Status != models.RunStatusAggregating {
			return nil
		}

		agg, err := s.repo.GetAggregation(ctx, expired.RunID)
		if err != nil {
			return err
		}
		if agg.Status != models.BatchStatusDispatched || agg.Attempt != expired.Attempt {
			return nil
		}

		_, err = s.retryOrKillAggregation(ctx, agg, reasonLeaseExpired)
		return err
	})
}

func (s *Service) resumeActiveRuns(ctx context.Context) {
	runs, err := s.repo.ListActiveRuns(ctx)
	if err != nil {
		s.log.Error("list active runs failed", slog.Any("error", err))
		return
	}

	for _, run := range runs {
		if ctx.Err() != nil {
			return
		}
		if err := s.resumeRun(ctx, run); err != nil {
			s.log.Error("resume run failed",
				slog.String("run_id", run.RunID),
				slog.String("status", string(run.Status)),
				slog.Any("error", err),
			)
		}
	}
}
