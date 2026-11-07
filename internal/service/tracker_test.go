package service

import (
	"context"
	"strings"
	"testing"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func batchEvent(status string, attempt int) events.BatchEvent {
	return events.BatchEvent{
		RunID:       testRunID,
		BatchID:     testBatchID,
		Attempt:     attempt,
		Status:      status,
		OutputLinks: []models.OutputLink{{URI: "s3://data-lake/prepared/x/part-0.parquet"}},
		Stats:       map[string]int64{"deals_in": 30, "deals_ok": 29, "deals_rejected": 1},
	}
}

func expectFreshEvent(d *testDeps, run *models.Run, stage models.Stage, attempt int) {
	d.repo.On("LockRun", mock.Anything, testRunID).Return(run, nil).Once()
	d.repo.On("MarkEventProcessed", mock.Anything, testRunID, testBatchID, stage, attempt).Return(true, nil).Once()
}

func TestHandleBatchEventSuccess(t *testing.T) {
	d := newTestService(t)
	event := batchEvent(events.StatusSuccess, 1)

	expectFreshEvent(d, runningRun(models.StagePrep), models.StagePrep, 1)
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePrep).
		Return(dispatchedStage(models.StagePrep, 1), nil).Once()
	d.repo.On("MarkBatchDone", mock.Anything, testRunID, testBatchID, models.StagePrep, event.OutputLinks, event.Stats).
		Return(nil).Once()
	d.repo.On("ListStageProgress", mock.Anything, testRunID, []models.Stage{models.StagePrep}).
		Return([]models.StageProgress{{Stage: models.StagePrep, Total: 2, Done: 1}}, nil).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePrep, event)
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "UpdateRunStage", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleBatchEventErrorRetries(t *testing.T) {
	d := newTestService(t)

	expectFreshEvent(d, runningRun(models.StageCashflow), models.StageCashflow, 1)
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StageCashflow).
		Return(dispatchedStage(models.StageCashflow, 1), nil).Once()
	d.repo.On("MarkBatchRetry", mock.Anything, testRunID, testBatchID, models.StageCashflow, "worker reported error on attempt 1").
		Return(nil).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StageCashflow, batchEvent(events.StatusError, 1))
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "FailRun", mock.Anything, mock.Anything, mock.Anything)
	d.repo.AssertNotCalled(t, "ListStageProgress", mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleBatchEventErrorOnLastAttemptKillsRun(t *testing.T) {
	d := newTestService(t)

	expectFreshEvent(d, runningRun(models.StagePV), models.StagePV, testMaxAttempts)
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePV).
		Return(dispatchedStage(models.StagePV, testMaxAttempts), nil).Once()
	d.repo.On("MarkBatchDead", mock.Anything, testRunID, testBatchID, models.StagePV, mock.Anything).Return(nil).Once()
	d.repo.On("FailRun", mock.Anything, testRunID, mock.MatchedBy(func(reason string) bool {
		return strings.Contains(reason, testBatchID) && strings.Contains(reason, "3 attempts")
	})).Return(nil).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePV, batchEvent(events.StatusError, testMaxAttempts))
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "MarkBatchRetry", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleBatchEventDuplicateIgnored(t *testing.T) {
	d := newTestService(t)

	d.repo.On("LockRun", mock.Anything, testRunID).Return(runningRun(models.StagePrep), nil).Once()
	d.repo.On("MarkEventProcessed", mock.Anything, testRunID, testBatchID, models.StagePrep, 1).Return(false, nil).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePrep, batchEvent(events.StatusSuccess, 1))
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "GetBatchStage", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleBatchEventStaleAttemptIgnored(t *testing.T) {
	d := newTestService(t)

	expectFreshEvent(d, runningRun(models.StagePrep), models.StagePrep, 1)
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePrep).
		Return(dispatchedStage(models.StagePrep, 2), nil).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePrep, batchEvent(events.StatusSuccess, 1))
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "MarkBatchDone", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleBatchEventUnknownBatch(t *testing.T) {
	d := newTestService(t)

	expectFreshEvent(d, runningRun(models.StagePrep), models.StagePrep, 1)
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePrep).
		Return(nil, models.ErrBatchNotFound).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePrep, batchEvent(events.StatusSuccess, 1))
	require.NoError(t, err)
}

func TestHandleBatchEventUnknownRun(t *testing.T) {
	d := newTestService(t)

	d.repo.On("LockRun", mock.Anything, testRunID).Return(nil, models.ErrRunNotFound).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePrep, batchEvent(events.StatusSuccess, 1))
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "MarkEventProcessed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleBatchEventCanceledRunIgnored(t *testing.T) {
	d := newTestService(t)

	run := runningRun(models.StagePrep)
	run.Status = models.RunStatusCanceled
	expectFreshEvent(d, run, models.StagePrep, 1)

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePrep, batchEvent(events.StatusSuccess, 1))
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "GetBatchStage", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleBatchEventInvalidIDsIgnored(t *testing.T) {
	d := newTestService(t)

	event := batchEvent(events.StatusSuccess, 1)
	event.BatchID = "not-a-uuid"

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePrep, event)
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "LockRun", mock.Anything, mock.Anything)
}

func TestBarrierClosesOnLastEvent(t *testing.T) {
	d := newTestService(t)
	event := batchEvent(events.StatusSuccess, 1)

	expectFreshEvent(d, runningRun(models.StagePrep), models.StagePrep, 1)
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePrep).
		Return(dispatchedStage(models.StagePrep, 1), nil).Once()
	d.repo.On("MarkBatchDone", mock.Anything, testRunID, testBatchID, models.StagePrep, mock.Anything, mock.Anything).
		Return(nil).Once()
	d.repo.On("ListStageProgress", mock.Anything, testRunID, []models.Stage{models.StagePrep}).
		Return([]models.StageProgress{{Stage: models.StagePrep, Total: 2, Done: 2}}, nil).Once()
	d.repo.On("UpdateRunStage", mock.Anything, testRunID, models.RunStatusRunning, models.StageCashflow).Return(nil).Once()

	d.repo.On("ListPendingBatchIDs", mock.Anything, testRunID, models.StageCashflow).Return([]string{testBatchID}, nil).Once()
	d.repo.On("LockRun", mock.Anything, testRunID).Return(runningRun(models.StageCashflow), nil).Once()
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StageCashflow).
		Return(&models.BatchStage{RunID: testRunID, BatchID: testBatchID, Stage: models.StageCashflow, Status: models.BatchStatusPending}, nil).Once()
	d.repo.On("MarkBatchDispatched", mock.Anything, testRunID, testBatchID, models.StageCashflow, 1, testLeaseTTL).Return(nil).Once()
	d.pub.On("PublishTask", mock.Anything, "cashflow.tasks", testRunID+":"+testBatchID+":cashflow:1", events.BatchTask{
		RunID:         testRunID,
		BatchID:       testBatchID,
		Attempt:       1,
		DealIDsLink:   "s3://data-lake/runs/" + testRunID + "/batches/" + testBatchID + ".ids",
		OutputPrefix:  "s3://data-lake/cashflow/" + testRunID + "/" + testBatchID + "/",
		EngineVersion: events.DefaultEngineVersion,
	}).Return(nil).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePrep, event)
	require.NoError(t, err)
}

func TestBarrierNotClosedWhileBatchesRemain(t *testing.T) {
	d := newTestService(t)

	expectFreshEvent(d, runningRun(models.StageCashflow), models.StageCashflow, 1)
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StageCashflow).
		Return(dispatchedStage(models.StageCashflow, 1), nil).Once()
	d.repo.On("MarkBatchDone", mock.Anything, testRunID, testBatchID, models.StageCashflow, mock.Anything, mock.Anything).
		Return(nil).Once()
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePrep).
		Return(&models.BatchStage{Stats: map[string]int64{"deals_ok": 29}}, nil).Once()
	d.repo.On("ListStageProgress", mock.Anything, testRunID, []models.Stage{models.StageCashflow}).
		Return([]models.StageProgress{{Stage: models.StageCashflow, Total: 3, Done: 2, Failed: 0}}, nil).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StageCashflow, batchEvent(events.StatusSuccess, 1))
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "UpdateRunStage", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBarrierNotClosedTwice(t *testing.T) {
	d := newTestService(t)

	expectFreshEvent(d, runningRun(models.StageCashflow), models.StagePrep, 1)
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePrep).
		Return(dispatchedStage(models.StagePrep, 1), nil).Once()
	d.repo.On("MarkBatchDone", mock.Anything, testRunID, testBatchID, models.StagePrep, mock.Anything, mock.Anything).
		Return(nil).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePrep, batchEvent(events.StatusSuccess, 1))
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "ListStageProgress", mock.Anything, mock.Anything, mock.Anything)
	d.repo.AssertNotCalled(t, "UpdateRunStage", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBarrierAfterPVStartsAggregation(t *testing.T) {
	d := newTestService(t)

	expectFreshEvent(d, runningRun(models.StagePV), models.StagePV, 1)
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePV).
		Return(dispatchedStage(models.StagePV, 1), nil).Once()
	d.repo.On("MarkBatchDone", mock.Anything, testRunID, testBatchID, models.StagePV, mock.Anything, mock.Anything).
		Return(nil).Once()
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StageCashflow).
		Return(&models.BatchStage{Stats: map[string]int64{"deals_ok": 30}}, nil).Once()
	d.repo.On("ListStageProgress", mock.Anything, testRunID, []models.Stage{models.StagePV}).
		Return([]models.StageProgress{{Stage: models.StagePV, Total: 1, Done: 1}}, nil).Once()
	d.repo.On("UpdateRunStage", mock.Anything, testRunID, models.RunStatusAggregating, models.StageAggregate).Return(nil).Once()
	d.repo.On("EnsureAggregation", mock.Anything, testRunID, testPortfolioID).Return(nil).Once()

	d.repo.On("GetRun", mock.Anything, testRunID).Return(runningRun(models.StagePV), nil).Once()

	err := d.svc.HandleBatchEvent(context.Background(), models.StagePV, batchEvent(events.StatusSuccess, 1))
	require.NoError(t, err)
}

func TestDispatchStagePublishErrorKeepsBatchPending(t *testing.T) {
	d := newTestService(t)

	d.repo.On("ListPendingBatchIDs", mock.Anything, testRunID, models.StagePrep).Return([]string{testBatchID}, nil).Once()
	d.repo.On("LockRun", mock.Anything, testRunID).Return(runningRun(models.StagePrep), nil).Once()
	d.repo.On("GetBatchStage", mock.Anything, testRunID, testBatchID, models.StagePrep).
		Return(&models.BatchStage{Status: models.BatchStatusPending, Attempt: 1}, nil).Once()
	d.repo.On("MarkBatchDispatched", mock.Anything, testRunID, testBatchID, models.StagePrep, 2, 2*testLeaseTTL).Return(nil).Once()
	d.pub.On("PublishTask", mock.Anything, "data-prep.tasks", testRunID+":"+testBatchID+":prep:2", mock.Anything).
		Return(context.DeadlineExceeded).Once()

	err := d.svc.DispatchStage(context.Background(), testRunID, models.StagePrep)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDispatchStageSkipsCanceledRun(t *testing.T) {
	d := newTestService(t)

	run := runningRun(models.StagePrep)
	run.Status = models.RunStatusCanceled

	d.repo.On("ListPendingBatchIDs", mock.Anything, testRunID, models.StagePrep).Return([]string{testBatchID}, nil).Once()
	d.repo.On("LockRun", mock.Anything, testRunID).Return(run, nil).Once()

	err := d.svc.DispatchStage(context.Background(), testRunID, models.StagePrep)
	require.NoError(t, err)
	d.pub.AssertNotCalled(t, "PublishTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestLeaseFor(t *testing.T) {
	d := newTestService(t)

	tests := []struct {
		attempt int
		want    int64
	}{
		{1, int64(testLeaseTTL)},
		{2, int64(2 * testLeaseTTL)},
		{3, int64(4 * testLeaseTTL)},
		{4, int64(maxLease)},
		{60, int64(maxLease)},
	}

	for _, tt := range tests {
		require.Equal(t, tt.want, int64(d.svc.leaseFor(tt.attempt)), "attempt %d", tt.attempt)
	}
}
