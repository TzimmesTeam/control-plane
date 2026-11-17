package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/storage/s3"
)

func (s *Service) HandleResult(ctx context.Context, stage models.Stage, event events.Result) error {
	if _, err := uuid.Parse(event.TaskID); err != nil {
		return fmt.Errorf("%w: invalid task_id", models.ErrInvalid)
	}
	if event.Status != "success" && event.Status != "error" {
		return fmt.Errorf("%w: status must be success or error", models.ErrInvalid)
	}
	if event.Status == "success" {
		if stage == models.Aggregate {
			if _, _, err := s3.ParseURI(event.ReportLink); err != nil || strings.HasSuffix(event.ReportLink, "/") {
				return fmt.Errorf("%w: missing S3 report_link", models.ErrInvalid)
			}
		} else if len(event.OutputLinks) == 0 {
			return fmt.Errorf("%w: missing output_links", models.ErrInvalid)
		}
		for _, link := range event.OutputLinks {
			if _, _, err := s3.ParseURI(link.URI); err != nil {
				return fmt.Errorf("%w: invalid output link", models.ErrInvalid)
			}
		}
	}
	return s.transition(ctx, stage, event, false)
}
func (s *Service) expire(ctx context.Context, id string) error {
	return s.transition(ctx, "", events.Result{TaskID: id, Status: "error", Error: "worker deadline exceeded"}, true)
}
func (s *Service) transition(ctx context.Context, stage models.Stage, event events.Result, expired bool) error {
	return s.repo.WithinTx(ctx, func(ctx context.Context) error {
		delivery, err := s.repo.GetTask(ctx, event.TaskID)
		if errors.Is(err, models.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		run, err := s.repo.LockRun(ctx, delivery.Task.RunID)
		if err != nil {
			return err
		}
		delivery, err = s.repo.GetTask(ctx, event.TaskID)
		if err != nil {
			return err
		}
		task := delivery.Task
		if stage != "" && stage != task.Stage {
			return fmt.Errorf("%w: event subject does not match task stage", models.ErrInvalid)
		}
		if run.State.Terminal() || delivery.State == "done" {
			return nil
		}
		if expired && (delivery.State != "sent" || time.Now().Before(delivery.LeaseUntil)) {
			return nil
		}
		var batch *models.Batch
		if task.Stage == models.Aggregate {
			if run.State != models.Aggregating || run.TaskID != task.ID {
				return nil
			}
		} else {
			batch, err = s.repo.GetBatch(ctx, run.ID, task.BatchID)
			if err != nil {
				return err
			}
			if run.State != models.Running || batch.TaskID != task.ID || batch.Stage != task.Stage {
				return nil
			}
		}
		if err := s.repo.FinishTask(ctx, task.ID); err != nil {
			return err
		}
		if event.Status == "error" {
			return s.retry(ctx, run, batch, task, event.Error)
		}
		if batch == nil {
			run.State = models.Succeeded
			run.ReportLink = event.ReportLink
			run.Error = ""
			return s.repo.SaveRun(ctx, run)
		}
		batch.Error = ""
		if task.Stage == models.Cashflow {
			batch.CashflowLinks = event.OutputLinks
			batch.Stage = models.PV
			next := s.newTask(run, batch, models.PV, 1)
			batch.TaskID = next.ID
			if err := s.repo.AddTask(ctx, next); err != nil {
				return err
			}
		} else {
			batch.PVLinks = event.OutputLinks
			batch.Stage = models.Done
		}
		if err := s.repo.SaveBatch(ctx, batch); err != nil {
			return err
		}
		remaining, err := s.repo.RemainingBatches(ctx, run.ID)
		if err != nil {
			return err
		}
		if remaining == 0 {
			run.State = models.Aggregating
			return s.repo.SaveRun(ctx, run)
		}
		return nil
	})
}
func (s *Service) retry(ctx context.Context, run *models.Run, batch *models.Batch, task models.Task, reason string) error {
	if reason == "" {
		reason = "worker reported error"
	}
	if task.Attempt >= s.cfg.MaxAttempts {
		run.State = models.Failed
		run.Error = fmt.Sprintf("%s task %s failed after %d attempts: %s", task.Stage, task.ID, task.Attempt, reason)
		if batch != nil {
			batch.Stage = models.Stage(models.Failed)
			batch.Error = reason
			if err := s.repo.SaveBatch(ctx, batch); err != nil {
				return err
			}
		}
		return s.repo.SaveRun(ctx, run)
	}
	next := s.newTask(run, batch, task.Stage, task.Attempt+1)
	next.BatchesURI = task.BatchesURI
	if err := s.repo.AddTask(ctx, next); err != nil {
		return err
	}
	if batch != nil {
		batch.TaskID = next.ID
		batch.Error = reason
		return s.repo.SaveBatch(ctx, batch)
	}
	run.TaskID = next.ID
	run.Error = reason
	return s.repo.SaveRun(ctx, run)
}
