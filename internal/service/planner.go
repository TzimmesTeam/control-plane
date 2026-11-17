package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/storage/s3"
)

func (s *Service) prepare(ctx context.Context, id string) error {
	var run *models.Run
	err := s.repo.WithinTx(ctx, func(ctx context.Context) error {
		current, err := s.repo.LockRun(ctx, id)
		if err != nil {
			return err
		}
		if !current.Ready || current.PortfolioID == "" || current.State != models.Waiting && !(current.State == models.Preparing && time.Now().After(current.PlanningUntil)) {
			return nil
		}
		if current.PlanningAttempts >= s.cfg.MaxAttempts {
			current.State = models.Failed
			current.Error = "preparation attempts exhausted"
			return s.repo.SaveRun(ctx, current)
		}
		current.State = models.Preparing
		current.PlanningToken = uuid.NewString()
		current.PlanningAttempts++
		current.PlanningUntil = time.Now().Add(s.cfg.LeaseTTL)
		if err := s.repo.SaveRun(ctx, current); err != nil {
			return err
		}
		run = current
		return nil
	})
	if err != nil || run == nil {
		return err
	}
	planCtx, cancel := context.WithDeadline(ctx, run.PlanningUntil)
	defer cancel()
	uri := run.PreparedURI
	if uri == "" {
		uri, err = s.store.Resolve(planCtx, run.SnapshotID)
	}
	run.PreparedURI = uri
	var batches []models.Batch
	if err == nil {
		batches, run.SourceVersion, run.SourceETag, err = s.store.Plan(planCtx, run, s.cfg.BatchSize, s.cfg.MaxBatches)
	}
	if err != nil {
		cause := err
		saveErr := s.repo.WithinTx(ctx, func(ctx context.Context) error {
			current, err := s.repo.LockRun(ctx, id)
			if err != nil {
				return err
			}
			if current.State != models.Preparing || current.PlanningToken != run.PlanningToken {
				return nil
			}
			current.State = models.Waiting
			current.Error = cause.Error()
			if current.PlanningAttempts >= s.cfg.MaxAttempts {
				current.State = models.Failed
			}
			return s.repo.SaveRun(ctx, current)
		})
		if saveErr != nil {
			return fmt.Errorf("prepare: %v; save failure: %w", cause, saveErr)
		}
		return cause
	}
	return s.repo.WithinTx(ctx, func(ctx context.Context) error {
		current, err := s.repo.LockRun(ctx, id)
		if err != nil {
			return err
		}
		if current.State != models.Preparing || current.PlanningToken != run.PlanningToken {
			return nil
		}
		current.PreparedURI = run.PreparedURI
		current.SourceVersion = run.SourceVersion
		current.SourceETag = run.SourceETag
		current.TotalBatches = len(batches)
		current.TotalDeals = 0
		current.State = models.Running
		current.Error = ""
		current.PlanningToken = ""
		current.PlanningUntil = time.Time{}
		for i := range batches {
			batch := &batches[i]
			current.TotalDeals += batch.DealCount
			task := s.newTask(current, batch, models.Cashflow, 1)
			batch.Stage = models.Cashflow
			batch.TaskID = task.ID
			if err := s.repo.SaveBatch(ctx, batch); err != nil {
				return err
			}
			if err := s.repo.AddTask(ctx, task); err != nil {
				return err
			}
		}
		if len(batches) == 0 {
			current.State = models.Failed
			current.Error = "prepared snapshot contains no accepted deals"
		}
		return s.repo.SaveRun(ctx, current)
	})
}

func (s *Service) newTask(run *models.Run, batch *models.Batch, stage models.Stage, attempt int) models.Task {
	task := models.Task{RunID: run.ID, SnapshotID: run.SnapshotID, PortfolioID: run.PortfolioID, ReportDate: run.ReportDate,
		SourceURI: run.PreparedURI, SourceVersion: run.SourceVersion, SourceETag: run.SourceETag,
		CurvesURI: run.CurvesURI, EngineVersion: run.EngineVersion, Stage: stage, Attempt: attempt}
	if batch != nil {
		task.BatchID = batch.ID
		task.IDsURI = batch.IDsURI
		if stage == models.PV {
			task.InputLinks = batch.CashflowLinks
		}
	}
	task.ID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("%s/%s/%s/%d", run.ID, task.BatchID, stage, attempt))).String()
	bucket := s.cfg.CuratedBucket
	directory := "cf_projection"
	if stage == models.PV {
		directory = "pv_results"
	}
	if stage == models.Aggregate {
		bucket = s.cfg.ArtifactsBucket
		directory = "reports"
	}
	task.OutputPrefix = s3.URI(bucket, fmt.Sprintf("%s/run_id=%s/task_id=%s/", directory, run.ID, task.ID))
	return task
}

func (s *Service) aggregate(ctx context.Context, run models.Run) error {
	batches, err := s.repo.ListBatches(ctx, run.ID)
	if err != nil {
		return err
	}
	if len(batches) == 0 {
		return fmt.Errorf("aggregation has no batches")
	}
	for _, batch := range batches {
		if batch.Stage != models.Done {
			return fmt.Errorf("aggregation contains unfinished batch %s", batch.ID)
		}
	}
	uri := s3.URI(s.cfg.ArtifactsBucket, "runs/"+run.ID+"/manifests/batches.json")
	if err := s.store.PutJSON(ctx, uri, batches); err != nil {
		return err
	}
	return s.repo.WithinTx(ctx, func(ctx context.Context) error {
		current, err := s.repo.LockRun(ctx, run.ID)
		if err != nil {
			return err
		}
		if current.State != models.Aggregating || current.TaskID != "" {
			return nil
		}
		task := s.newTask(current, nil, models.Aggregate, 1)
		task.BatchesURI = uri
		current.TaskID = task.ID
		if err := s.repo.AddTask(ctx, task); err != nil {
			return err
		}
		return s.repo.SaveRun(ctx, current)
	})
}
