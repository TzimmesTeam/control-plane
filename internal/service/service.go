package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

type Repository interface {
	WithinTx(context.Context, func(context.Context) error) error
	EnsureRun(context.Context, string, string) error
	GetRun(context.Context, string) (*models.Run, error)
	LockRun(context.Context, string) (*models.Run, error)
	SaveRun(context.Context, *models.Run) error
	ActiveRuns(context.Context, int) ([]models.Run, error)
	SaveBatch(context.Context, *models.Batch) error
	GetBatch(context.Context, string, string) (*models.Batch, error)
	ListBatches(context.Context, string) ([]models.Batch, error)
	RemainingBatches(context.Context, string) (int, error)
	AddTask(context.Context, models.Task) error
	GetTask(context.Context, string) (*models.Delivery, error)
	FinishTask(context.Context, string) error
	ClaimTasks(context.Context, int, time.Duration) ([]models.Task, error)
	MarkSent(context.Context, string, time.Duration) error
	ReleaseTask(context.Context, string) error
	ExpiredTasks(context.Context, int) ([]models.Task, error)
}
type ObjectStore interface {
	Resolve(context.Context, string) (string, error)
	Plan(context.Context, *models.Run, int, int) ([]models.Batch, string, string, error)
	PutJSON(context.Context, string, any) error
}
type Publisher interface {
	PublishTask(context.Context, string, string, any) error
}
type Settings struct {
	BatchSize          int
	MaxBatches         int
	MaxAttempts        int
	ScanInterval       time.Duration
	LeaseTTL           time.Duration
	PublishTimeout     time.Duration
	ArtifactsBucket    string
	CuratedBucket      string
	TaskSubjects       map[models.Stage]string
	DefaultPortfolioID string
	DefaultReportDate  string
	DefaultCurvesURI   string
}
type Service struct {
	repo  Repository
	store ObjectStore
	pub   Publisher
	cfg   Settings
	log   *slog.Logger
}

func NewService(log *slog.Logger, repo Repository, store ObjectStore, pub Publisher, cfg Settings) (*Service, error) {
	if repo == nil || store == nil || pub == nil {
		return nil, fmt.Errorf("service: missing dependency")
	}
	if cfg.BatchSize < 1 || cfg.MaxBatches < 1 || cfg.MaxAttempts < 1 || cfg.ScanInterval <= 0 || cfg.LeaseTTL <= 0 || cfg.PublishTimeout <= 0 || cfg.ArtifactsBucket == "" || cfg.CuratedBucket == "" {
		return nil, fmt.Errorf("service: invalid settings")
	}
	for _, stage := range []models.Stage{models.Cashflow, models.PV, models.Aggregate} {
		if cfg.TaskSubjects[stage] == "" {
			return nil, fmt.Errorf("service: missing subject for %s", stage)
		}
	}
	if log == nil {
		log = slog.Default()
	}
	if cfg.DefaultPortfolioID == "" {
		cfg.DefaultPortfolioID = "{snapshot_id}"
	}
	if cfg.DefaultReportDate == "" {
		cfg.DefaultReportDate = "1970-01-01"
	}
	if cfg.DefaultCurvesURI == "" {
		cfg.DefaultCurvesURI = "s3://" + cfg.CuratedBucket + "/curve_snapshots/default/curves.parquet"
	}
	return &Service{repo: repo, store: store, pub: pub, cfg: cfg, log: log}, nil
}

func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		s.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (s *Service) Tick(ctx context.Context) {
	runs, err := s.repo.ActiveRuns(ctx, 100)
	if err != nil {
		s.log.Error("load active runs", "error", err)
		return
	}
	for _, run := range runs {
		if ctx.Err() != nil {
			return
		}
		switch {
		case run.Ready && run.PortfolioID != "" && (run.State == models.Waiting || run.State == models.Preparing && time.Now().After(run.PlanningUntil)):
			err = s.prepare(ctx, run.ID)
		case run.State == models.Aggregating && run.TaskID == "":
			err = s.aggregate(ctx, run)
		default:
			err = nil
		}
		if err != nil {
			s.log.Error("advance run", "run_id", run.ID, "error", err)
		}
	}
	expired, err := s.repo.ExpiredTasks(ctx, 100)
	if err != nil {
		s.log.Error("load expired tasks", "error", err)
	} else {
		for _, task := range expired {
			if err := s.expire(ctx, task.ID); err != nil {
				s.log.Error("expire task", "task_id", task.ID, "error", err)
			}
		}
	}
	s.dispatch(ctx)
}
func (s *Service) dispatch(ctx context.Context) {
	for i := 0; i < 100 && ctx.Err() == nil; i++ {
		tasks, err := s.repo.ClaimTasks(ctx, 1, s.cfg.PublishTimeout*2)
		if err != nil {
			s.log.Error("claim tasks", "error", err)
			return
		}
		if len(tasks) == 0 {
			return
		}
		task := tasks[0]
		publishCtx, cancel := context.WithTimeout(ctx, s.cfg.PublishTimeout)
		err = s.pub.PublishTask(publishCtx, s.cfg.TaskSubjects[task.Stage], task.ID, task)
		cancel()
		if err == nil {
			err = s.repo.MarkSent(ctx, task.ID, s.cfg.LeaseTTL)
		} else {
			if releaseErr := s.repo.ReleaseTask(ctx, task.ID); releaseErr != nil {
				s.log.Error("release task", "task_id", task.ID, "error", releaseErr)
			}
		}
		if err != nil {
			s.log.Error("publish task; will retry", "task_id", task.ID, "error", err)
			return
		}
	}
}
