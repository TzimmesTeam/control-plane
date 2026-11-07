package service

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func ret[T any](args mock.Arguments, i int) T {
	v, ok := args.Get(i).(T)
	if !ok {
		var zero T
		return zero
	}

	return v
}

type repositoryMock struct{ mock.Mock }

func (m *repositoryMock) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

func (m *repositoryMock) CreateRun(ctx context.Context, run *models.Run) error {
	return m.Called(ctx, run).Error(0)
}

func (m *repositoryMock) GetRun(ctx context.Context, runID string) (*models.Run, error) {
	args := m.Called(ctx, runID)
	return ret[*models.Run](args, 0), args.Error(1)
}

func (m *repositoryMock) LockRun(ctx context.Context, runID string) (*models.Run, error) {
	args := m.Called(ctx, runID)
	return ret[*models.Run](args, 0), args.Error(1)
}

func (m *repositoryMock) ListActiveRuns(ctx context.Context) ([]*models.Run, error) {
	args := m.Called(ctx)
	return ret[[]*models.Run](args, 0), args.Error(1)
}

func (m *repositoryMock) UpdateRunStatus(ctx context.Context, runID string, status models.RunStatus) error {
	return m.Called(ctx, runID, status).Error(0)
}

func (m *repositoryMock) UpdateRunStage(ctx context.Context, runID string, status models.RunStatus, stage models.Stage) error {
	return m.Called(ctx, runID, status, stage).Error(0)
}

func (m *repositoryMock) StartRunStages(ctx context.Context, runID, contextURI string, totalDeals, totalBatches int) error {
	return m.Called(ctx, runID, contextURI, totalDeals, totalBatches).Error(0)
}

func (m *repositoryMock) FailRun(ctx context.Context, runID, reason string) error {
	return m.Called(ctx, runID, reason).Error(0)
}

func (m *repositoryMock) CompleteRun(ctx context.Context, runID, reportLink string) error {
	return m.Called(ctx, runID, reportLink).Error(0)
}

func (m *repositoryMock) CancelRun(ctx context.Context, runID string) error {
	return m.Called(ctx, runID).Error(0)
}

func (m *repositoryMock) InsertBatch(ctx context.Context, batch *models.Batch) error {
	return m.Called(ctx, batch).Error(0)
}

func (m *repositoryMock) ListBatches(ctx context.Context, runID string) ([]*models.Batch, error) {
	args := m.Called(ctx, runID)
	return ret[[]*models.Batch](args, 0), args.Error(1)
}

func (m *repositoryMock) ListBatchStages(ctx context.Context, runID string, filter models.BatchStageFilter) ([]*models.BatchStage, error) {
	args := m.Called(ctx, runID, filter)
	return ret[[]*models.BatchStage](args, 0), args.Error(1)
}

func (m *repositoryMock) GetBatchStage(ctx context.Context, runID, batchID string, stage models.Stage) (*models.BatchStage, error) {
	args := m.Called(ctx, runID, batchID, stage)
	return ret[*models.BatchStage](args, 0), args.Error(1)
}

func (m *repositoryMock) ListPendingBatchIDs(ctx context.Context, runID string, stage models.Stage) ([]string, error) {
	args := m.Called(ctx, runID, stage)
	return ret[[]string](args, 0), args.Error(1)
}

func (m *repositoryMock) MarkBatchDispatched(
	ctx context.Context,
	runID, batchID string,
	stage models.Stage,
	attempt int,
	lease time.Duration,
) error {
	return m.Called(ctx, runID, batchID, stage, attempt, lease).Error(0)
}

func (m *repositoryMock) MarkBatchDone(
	ctx context.Context,
	runID, batchID string,
	stage models.Stage,
	links []models.OutputLink,
	stats map[string]int64,
) error {
	return m.Called(ctx, runID, batchID, stage, links, stats).Error(0)
}

func (m *repositoryMock) MarkBatchRetry(ctx context.Context, runID, batchID string, stage models.Stage, reason string) error {
	return m.Called(ctx, runID, batchID, stage, reason).Error(0)
}

func (m *repositoryMock) MarkBatchDead(ctx context.Context, runID, batchID string, stage models.Stage, reason string) error {
	return m.Called(ctx, runID, batchID, stage, reason).Error(0)
}

func (m *repositoryMock) ListStageProgress(ctx context.Context, runID string, stages ...models.Stage) ([]models.StageProgress, error) {
	args := m.Called(ctx, runID, stages)
	return ret[[]models.StageProgress](args, 0), args.Error(1)
}

func (m *repositoryMock) MarkEventProcessed(ctx context.Context, runID, batchID string, stage models.Stage, attempt int) (bool, error) {
	args := m.Called(ctx, runID, batchID, stage, attempt)
	return args.Bool(0), args.Error(1)
}

func (m *repositoryMock) ListExpiredBatchStages(ctx context.Context, limit int) ([]*models.BatchStage, error) {
	args := m.Called(ctx, limit)
	return ret[[]*models.BatchStage](args, 0), args.Error(1)
}

func (m *repositoryMock) ListBatchOutputs(ctx context.Context, runID string) ([]models.BatchOutputs, error) {
	args := m.Called(ctx, runID)
	return ret[[]models.BatchOutputs](args, 0), args.Error(1)
}

func (m *repositoryMock) EnsureAggregation(ctx context.Context, runID, portfolioID string) error {
	return m.Called(ctx, runID, portfolioID).Error(0)
}

func (m *repositoryMock) GetAggregation(ctx context.Context, runID string) (*models.Aggregation, error) {
	args := m.Called(ctx, runID)
	return ret[*models.Aggregation](args, 0), args.Error(1)
}

func (m *repositoryMock) MarkAggregationDispatched(
	ctx context.Context,
	runID string,
	attempt int,
	lease time.Duration,
	batchesURI string,
) error {
	return m.Called(ctx, runID, attempt, lease, batchesURI).Error(0)
}

func (m *repositoryMock) MarkAggregationDone(ctx context.Context, runID, reportLink string, stats map[string]int64) error {
	return m.Called(ctx, runID, reportLink, stats).Error(0)
}

func (m *repositoryMock) MarkAggregationRetry(ctx context.Context, runID, reason string) error {
	return m.Called(ctx, runID, reason).Error(0)
}

func (m *repositoryMock) MarkAggregationDead(ctx context.Context, runID, reason string) error {
	return m.Called(ctx, runID, reason).Error(0)
}

func (m *repositoryMock) ListExpiredAggregations(ctx context.Context, limit int) ([]*models.Aggregation, error) {
	args := m.Called(ctx, limit)
	return ret[[]*models.Aggregation](args, 0), args.Error(1)
}

type publisherMock struct{ mock.Mock }

func (m *publisherMock) PublishTask(ctx context.Context, subject, msgID string, msg any) error {
	return m.Called(ctx, subject, msgID, msg).Error(0)
}

type objectStoreMock struct{ mock.Mock }

func (m *objectStoreMock) Put(ctx context.Context, bucket, key string, data []byte) error {
	return m.Called(ctx, bucket, key, data).Error(0)
}

func (m *objectStoreMock) Open(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	args := m.Called(ctx, bucket, key)
	return ret[io.ReadCloser](args, 0), args.Error(1)
}

func (m *objectStoreMock) Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) error {
	return m.Called(ctx, srcBucket, srcKey, dstBucket, dstKey).Error(0)
}

func (m *objectStoreMock) CopyPrefix(ctx context.Context, srcBucket, srcPrefix, dstBucket, dstPrefix string) (int, error) {
	args := m.Called(ctx, srcBucket, srcPrefix, dstBucket, dstPrefix)
	return args.Int(0), args.Error(1)
}

const (
	testReport      = "report-store"
	testLeaseTTL    = 10 * time.Minute
	testMaxAttempts = 3
)

type testDeps struct {
	repo  *repositoryMock
	store *objectStoreMock
	pub   *publisherMock
	svc   *Service
}

func newTestService(t *testing.T) *testDeps {
	t.Helper()

	d := &testDeps{repo: &repositoryMock{}, store: &objectStoreMock{}, pub: &publisherMock{}}

	svc, err := NewService(testLogger(), d.repo, d.store, d.pub, Settings{
		LakeBucket:        testLake,
		ReportBucket:      testReport,
		BatchTargetWeight: 1000,
		MaxBatchesPerRun:  100,
		MaxAttempts:       testMaxAttempts,
		LeaseTTL:          testLeaseTTL,
		ScanInterval:      time.Second,
		TaskSubjects: map[models.Stage]string{
			models.StagePrep:      "data-prep.tasks",
			models.StageCashflow:  "cashflow.tasks",
			models.StagePV:        "pv.tasks",
			models.StageAggregate: "aggregator.tasks",
		},
	})
	require.NoError(t, err)

	d.svc = svc
	t.Cleanup(func() {
		d.repo.AssertExpectations(t)
		d.store.AssertExpectations(t)
		d.pub.AssertExpectations(t)
	})

	return d
}

func runningRun(stage models.Stage) *models.Run {
	return &models.Run{
		RunID:        testRunID,
		PortfolioID:  testPortfolioID,
		ReportDttm:   time.Date(2022, 8, 31, 0, 0, 0, 0, time.UTC),
		Status:       models.RunStatusRunning,
		CurrentStage: stage,
	}
}

func dispatchedStage(stage models.Stage, attempt int) *models.BatchStage {
	return &models.BatchStage{
		RunID:     testRunID,
		BatchID:   testBatchID,
		Stage:     stage,
		Status:    models.BatchStatusDispatched,
		Attempt:   attempt,
		DealCount: 30,
	}
}
