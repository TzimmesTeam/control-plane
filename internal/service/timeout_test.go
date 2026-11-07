package service

import (
	"context"
	"testing"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/stretchr/testify/mock"
)

func TestExpiredLeaseReturnsBatchToPending(t *testing.T) {
	d := newTestService(t)

	d.repo.On("ListExpiredBatchStages", mock.Anything, scanBatchSize).
		Return([]*models.BatchStage{dispatchedStage(models.StagePrep, 1)}, nil).Once()
	d.repo.On("LockRun", mock.Anything, testRunID).Return(runningRun(models.StagePrep), nil).Once()
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePrep).
		Return(dispatchedStage(models.StagePrep, 1), nil).Once()
	d.repo.On("MarkBatchRetry", mock.Anything, testRunID, testBatchID, models.StagePrep, "lease expired on attempt 1").
		Return(nil).Once()

	d.svc.expireBatchLeases(context.Background())
}

func TestExpiredLeaseOnLastAttemptKillsRun(t *testing.T) {
	d := newTestService(t)

	d.repo.On("ListExpiredBatchStages", mock.Anything, scanBatchSize).
		Return([]*models.BatchStage{dispatchedStage(models.StagePrep, testMaxAttempts)}, nil).Once()
	d.repo.On("LockRun", mock.Anything, testRunID).Return(runningRun(models.StagePrep), nil).Once()
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePrep).
		Return(dispatchedStage(models.StagePrep, testMaxAttempts), nil).Once()
	d.repo.On("MarkBatchDead", mock.Anything, testRunID, testBatchID, models.StagePrep, "lease expired on attempt 3").
		Return(nil).Once()
	d.repo.On("FailRun", mock.Anything, testRunID, mock.Anything).Return(nil).Once()

	d.svc.expireBatchLeases(context.Background())
}

func TestExpiredLeaseIgnoredWhenEventArrivedMeanwhile(t *testing.T) {
	d := newTestService(t)

	done := dispatchedStage(models.StagePrep, 1)
	done.Status = models.BatchStatusDone

	d.repo.On("ListExpiredBatchStages", mock.Anything, scanBatchSize).
		Return([]*models.BatchStage{dispatchedStage(models.StagePrep, 1)}, nil).Once()
	d.repo.On("LockRun", mock.Anything, testRunID).Return(runningRun(models.StagePrep), nil).Once()
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePrep).Return(done, nil).Once()

	d.svc.expireBatchLeases(context.Background())
	d.repo.AssertNotCalled(t, "MarkBatchRetry", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestExpiredAggregationLeaseRetries(t *testing.T) {
	d := newTestService(t)

	d.repo.On("ListExpiredAggregations", mock.Anything, scanBatchSize).
		Return([]*models.Aggregation{dispatchedAggregation(1)}, nil).Once()
	d.repo.On("LockRun", mock.Anything, testRunID).Return(aggregatingRun(), nil).Once()
	d.repo.On("GetAggregation", mock.Anything, testRunID).Return(dispatchedAggregation(1), nil).Once()
	d.repo.On("MarkAggregationRetry", mock.Anything, testRunID, "lease expired on attempt 1").Return(nil).Once()

	d.svc.expireAggregationLeases(context.Background())
}
