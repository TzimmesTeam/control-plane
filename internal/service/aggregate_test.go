package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const testReportLink = "s3://report-store/" + testRunID + "/report.parquet"

func aggregatingRun() *models.Run {
	run := runningRun(models.StageAggregate)
	run.Status = models.RunStatusAggregating
	return run
}

func dispatchedAggregation(attempt int) *models.Aggregation {
	return &models.Aggregation{
		RunID:       testRunID,
		PortfolioID: testPortfolioID,
		Status:      models.BatchStatusDispatched,
		Attempt:     attempt,
	}
}

func aggregateEvent(status string, attempt int, missing ...string) events.AggregateEvent {
	return events.AggregateEvent{
		RunID:          testRunID,
		PortfolioID:    testPortfolioID,
		Attempt:        attempt,
		Status:         status,
		ReportLink:     testReportLink,
		MissingBatches: missing,
		Stats:          map[string]int64{"deals_total": 30, "batches": 1},
	}
}

func expectFreshAggregateEvent(d *testDeps, attempt int) {
	d.repo.On("LockRun", mock.Anything, testRunID).Return(aggregatingRun(), nil).Once()
	d.repo.On("MarkEventProcessed", mock.Anything, testRunID, aggregateEventBatchID, models.StageAggregate, attempt).
		Return(true, nil).Once()
	d.repo.On("GetAggregation", mock.Anything, testRunID).Return(dispatchedAggregation(attempt), nil).Once()
}

func TestHandleAggregateEventSuccess(t *testing.T) {
	d := newTestService(t)
	event := aggregateEvent(events.StatusSuccess, 1)

	expectFreshAggregateEvent(d, 1)
	d.repo.On("MarkAggregationDone", mock.Anything, testRunID, testReportLink, event.Stats).Return(nil).Once()
	d.repo.On("CompleteRun", mock.Anything, testRunID, testReportLink).Return(nil).Once()

	require.NoError(t, d.svc.HandleAggregateEvent(context.Background(), event))
}

func TestHandleAggregateEventMissingBatchesFailsRun(t *testing.T) {
	d := newTestService(t)

	expectFreshAggregateEvent(d, 1)
	d.repo.On("MarkAggregationDead", mock.Anything, testRunID, mock.Anything).Return(nil).Once()
	d.repo.On("FailRun", mock.Anything, testRunID, "aggregator reported missing batches: "+testBatchID).Return(nil).Once()

	err := d.svc.HandleAggregateEvent(context.Background(), aggregateEvent(events.StatusSuccess, 1, testBatchID))
	require.NoError(t, err)
	d.repo.AssertNotCalled(t, "CompleteRun", mock.Anything, mock.Anything, mock.Anything)
	d.repo.AssertNotCalled(t, "MarkAggregationRetry", mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleAggregateEventErrorRetries(t *testing.T) {
	d := newTestService(t)

	expectFreshAggregateEvent(d, 1)
	d.repo.On("MarkAggregationRetry", mock.Anything, testRunID, "worker reported error on attempt 1").Return(nil).Once()

	require.NoError(t, d.svc.HandleAggregateEvent(context.Background(), aggregateEvent(events.StatusError, 1)))
	d.repo.AssertNotCalled(t, "FailRun", mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleAggregateEventErrorOnLastAttemptFailsRun(t *testing.T) {
	d := newTestService(t)

	expectFreshAggregateEvent(d, testMaxAttempts)
	d.repo.On("MarkAggregationDead", mock.Anything, testRunID, mock.Anything).Return(nil).Once()
	d.repo.On("FailRun", mock.Anything, testRunID, mock.Anything).Return(nil).Once()

	require.NoError(t, d.svc.HandleAggregateEvent(context.Background(), aggregateEvent(events.StatusError, testMaxAttempts)))
}

func TestHandleAggregateEventPortfolioMismatchIgnored(t *testing.T) {
	d := newTestService(t)

	event := aggregateEvent(events.StatusSuccess, 1)
	event.PortfolioID = testBatchID

	d.repo.On("LockRun", mock.Anything, testRunID).Return(aggregatingRun(), nil).Once()

	require.NoError(t, d.svc.HandleAggregateEvent(context.Background(), event))
	d.repo.AssertNotCalled(t, "MarkEventProcessed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleAggregateEventStaleAttemptIgnored(t *testing.T) {
	d := newTestService(t)

	d.repo.On("LockRun", mock.Anything, testRunID).Return(aggregatingRun(), nil).Once()
	d.repo.On("MarkEventProcessed", mock.Anything, testRunID, aggregateEventBatchID, models.StageAggregate, 1).
		Return(true, nil).Once()
	d.repo.On("GetAggregation", mock.Anything, testRunID).Return(dispatchedAggregation(2), nil).Once()

	require.NoError(t, d.svc.HandleAggregateEvent(context.Background(), aggregateEvent(events.StatusSuccess, 1)))
	d.repo.AssertNotCalled(t, "CompleteRun", mock.Anything, mock.Anything, mock.Anything)
}

func TestStartAggregationWritesManifestAndPublishes(t *testing.T) {
	d := newTestService(t)

	manifestURI := "s3://data-lake/runs/" + testRunID + "/batches.json"
	cashflow := []models.OutputLink{{URI: "s3://data-lake/cashflow/a/part-0.parquet"}}
	pv := []models.OutputLink{{URI: "s3://data-lake/pv/a/part-0.parquet"}}

	d.repo.On("GetRun", mock.Anything, testRunID).Return(aggregatingRun(), nil).Once()
	d.repo.On("GetAggregation", mock.Anything, testRunID).
		Return(&models.Aggregation{RunID: testRunID, Status: models.BatchStatusPending}, nil).Twice()
	d.repo.On("ListBatchOutputs", mock.Anything, testRunID).Return([]models.BatchOutputs{
		{BatchID: testBatchID, DealCount: 30, Cashflow: cashflow, PV: pv},
	}, nil).Once()

	var manifest models.BatchesManifest
	d.store.On("Put", mock.Anything, testLake, "runs/"+testRunID+"/batches.json", mock.Anything).
		Run(func(args mock.Arguments) {
			data, ok := args.Get(3).([]byte)
			require.True(t, ok)
			require.NoError(t, json.Unmarshal(data, &manifest))
		}).Return(nil).Once()

	d.repo.On("LockRun", mock.Anything, testRunID).Return(aggregatingRun(), nil).Once()
	d.repo.On("MarkAggregationDispatched", mock.Anything, testRunID, 1, testLeaseTTL, manifestURI).Return(nil).Once()
	d.pub.On("PublishTask", mock.Anything, "aggregator.tasks", testRunID+":aggregate:1", events.AggregateTask{
		RunID:        testRunID,
		PortfolioID:  testPortfolioID,
		Attempt:      1,
		BatchesLink:  manifestURI,
		OutputPrefix: "s3://report-store/" + testRunID + "/",
	}).Return(nil).Once()

	require.NoError(t, d.svc.StartAggregation(context.Background(), testRunID))

	require.Equal(t, testRunID, manifest.RunID)
	require.Equal(t, testPortfolioID, manifest.PortfolioID)
	require.Equal(t, "2022-08-31", manifest.ReportDttm)
	require.Equal(t, 1, manifest.TotalBatches)
	require.Equal(t, []models.ManifestBatch{{BatchID: testBatchID, DealCount: 30, Cashflow: cashflow, PV: pv}}, manifest.Batches)
}

func TestStartAggregationSkipsDispatched(t *testing.T) {
	d := newTestService(t)

	d.repo.On("GetRun", mock.Anything, testRunID).Return(aggregatingRun(), nil).Once()
	d.repo.On("GetAggregation", mock.Anything, testRunID).Return(dispatchedAggregation(1), nil).Once()

	require.NoError(t, d.svc.StartAggregation(context.Background(), testRunID))
	d.pub.AssertNotCalled(t, "PublishTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
