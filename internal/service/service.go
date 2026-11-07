package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

const (
	runQueueSize  = 1024
	scanBatchSize = 100
	maxLease      = time.Hour
)

type Repository interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error

	RunRepository
	BatchRepository
	AggregationRepository
}

type RunRepository interface {
	CreateRun(ctx context.Context, run *models.Run) error
	GetRun(ctx context.Context, runID string) (*models.Run, error)
	LockRun(ctx context.Context, runID string) (*models.Run, error)
	ListActiveRuns(ctx context.Context) ([]*models.Run, error)
	UpdateRunStatus(ctx context.Context, runID string, status models.RunStatus) error
	UpdateRunStage(ctx context.Context, runID string, status models.RunStatus, stage models.Stage) error
	StartRunStages(ctx context.Context, runID, contextURI string, totalDeals, totalBatches int) error
	FailRun(ctx context.Context, runID, reason string) error
	CompleteRun(ctx context.Context, runID, reportLink string) error
	CancelRun(ctx context.Context, runID string) error
}

type BatchRepository interface {
	InsertBatch(ctx context.Context, batch *models.Batch) error
	ListBatches(ctx context.Context, runID string) ([]*models.Batch, error)
	ListBatchStages(ctx context.Context, runID string, filter models.BatchStageFilter) ([]*models.BatchStage, error)
	GetBatchStage(ctx context.Context, runID, batchID string, stage models.Stage) (*models.BatchStage, error)
	ListPendingBatchIDs(ctx context.Context, runID string, stage models.Stage) ([]string, error)
	MarkBatchDispatched(ctx context.Context, runID, batchID string, stage models.Stage, attempt int, lease time.Duration) error
	MarkBatchDone(ctx context.Context, runID, batchID string, stage models.Stage, links []models.OutputLink, stats map[string]int64) error
	MarkBatchRetry(ctx context.Context, runID, batchID string, stage models.Stage, reason string) error
	MarkBatchDead(ctx context.Context, runID, batchID string, stage models.Stage, reason string) error
	ListStageProgress(ctx context.Context, runID string, stages ...models.Stage) ([]models.StageProgress, error)
	MarkEventProcessed(ctx context.Context, runID, batchID string, stage models.Stage, attempt int) (bool, error)
	ListExpiredBatchStages(ctx context.Context, limit int) ([]*models.BatchStage, error)
	ListBatchOutputs(ctx context.Context, runID string) ([]models.BatchOutputs, error)
}

type AggregationRepository interface {
	EnsureAggregation(ctx context.Context, runID, portfolioID string) error
	GetAggregation(ctx context.Context, runID string) (*models.Aggregation, error)
	MarkAggregationDispatched(ctx context.Context, runID string, attempt int, lease time.Duration, batchesURI string) error
	MarkAggregationDone(ctx context.Context, runID, reportLink string, stats map[string]int64) error
	MarkAggregationRetry(ctx context.Context, runID, reason string) error
	MarkAggregationDead(ctx context.Context, runID, reason string) error
	ListExpiredAggregations(ctx context.Context, limit int) ([]*models.Aggregation, error)
}

type Publisher interface {
	PublishTask(ctx context.Context, subject, msgID string, msg any) error
}

type ObjectStore interface {
	Put(ctx context.Context, bucket, key string, data []byte) error
	Open(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) error
	CopyPrefix(ctx context.Context, srcBucket, srcPrefix, dstBucket, dstPrefix string) (int, error)
}

type Settings struct {
	LakeBucket        string
	ReportBucket      string
	BatchTargetWeight int64
	MaxBatchesPerRun  int
	MaxAttempts       int
	LeaseTTL          time.Duration
	ScanInterval      time.Duration
	TaskSubjects      map[models.Stage]string
}

type Service struct {
	log      *slog.Logger
	repo     Repository
	store    ObjectStore
	pub      Publisher
	splitter *Splitter
	cfg      Settings

	queue    chan string
	mu       sync.Mutex
	inflight map[string]struct{}
	wg       sync.WaitGroup
}

func NewService(log *slog.Logger, repo Repository, store ObjectStore, pub Publisher, cfg Settings) (*Service, error) {
	const op = "service.NewService"

	if log == nil {
		log = slog.Default()
	}
	if repo == nil || store == nil || pub == nil {
		return nil, fmt.Errorf("%s: repository, object store and publisher are required", op)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	splitter, err := NewSplitter(log, repo, store, SplitterConfig{
		Bucket:       cfg.LakeBucket,
		TargetWeight: cfg.BatchTargetWeight,
		MaxBatches:   cfg.MaxBatchesPerRun,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Service{
		log:      log,
		repo:     repo,
		store:    store,
		pub:      pub,
		splitter: splitter,
		cfg:      cfg,
		queue:    make(chan string, runQueueSize),
		inflight: make(map[string]struct{}),
	}, nil
}

func (c Settings) validate() error {
	if c.LakeBucket == "" || c.ReportBucket == "" {
		return errors.New("lake and report buckets are required")
	}
	if c.MaxAttempts < 1 {
		return errors.New("max attempts must be positive")
	}
	if c.LeaseTTL <= 0 || c.ScanInterval <= 0 {
		return errors.New("lease ttl and scan interval must be positive")
	}

	stages := append(models.BatchStages(), models.StageAggregate)
	for _, stage := range stages {
		if c.TaskSubjects[stage] == "" {
			return fmt.Errorf("task subject for stage %s is required", stage)
		}
	}

	return nil
}

func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.ScanInterval)
	defer ticker.Stop()
	defer s.wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return
		case runID := <-s.queue:
			s.spawn(ctx, runID)
		case <-ticker.C:
			s.scan(ctx)
		}
	}
}

func (s *Service) enqueue(runID string) {
	s.mu.Lock()
	if _, busy := s.inflight[runID]; busy {
		s.mu.Unlock()
		return
	}
	s.inflight[runID] = struct{}{}
	s.mu.Unlock()

	select {
	case s.queue <- runID:
	default:
		s.release(runID)
		s.log.Warn("run queue is full, run will be picked up by the scanner", slog.String("run_id", runID))
	}
}

func (s *Service) release(runID string) {
	s.mu.Lock()
	delete(s.inflight, runID)
	s.mu.Unlock()
}

func (s *Service) spawn(ctx context.Context, runID string) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.release(runID)

		s.processRun(ctx, runID)
	}()
}

func (s *Service) leaseFor(attempt int) time.Duration {
	lease := s.cfg.LeaseTTL
	for i := 1; i < attempt && lease < maxLease; i++ {
		lease *= 2
	}

	return min(lease, maxLease)
}
