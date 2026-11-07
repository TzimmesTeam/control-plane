package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func validInput() CreateRunInput {
	return CreateRunInput{
		PortfolioID: testPortfolioID,
		ReportDttm:  "2022-08-31",
		SourceURI:   "s3://data-lake/input/deals.csv",
		CurvesURI:   "s3://data-lake/input/curves",
	}
}

func TestCreateRunValidation(t *testing.T) {
	tests := []struct {
		name   string
		modify func(in *CreateRunInput)
		field  string
	}{
		{"bad portfolio", func(in *CreateRunInput) { in.PortfolioID = "42" }, "portfolio_id"},
		{"empty portfolio", func(in *CreateRunInput) { in.PortfolioID = "" }, "portfolio_id"},
		{"bad date format", func(in *CreateRunInput) { in.ReportDttm = "31.08.2022" }, "report_dttm"},
		{"impossible date", func(in *CreateRunInput) { in.ReportDttm = "2022-02-30" }, "report_dttm"},
		{"source without scheme", func(in *CreateRunInput) { in.SourceURI = "/tmp/deals.csv" }, "source_uri"},
		{"source is a prefix", func(in *CreateRunInput) { in.SourceURI = "s3://data-lake/input/" }, "source_uri"},
		{"empty curves", func(in *CreateRunInput) { in.CurvesURI = "" }, "curves_uri"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestService(t)

			in := validInput()
			tt.modify(&in)

			_, err := d.svc.CreateRun(context.Background(), in)

			var vErr *ValidationError
			require.ErrorAs(t, err, &vErr)
			require.Equal(t, tt.field, vErr.Field)
		})
	}
}

func TestCreateRunRegistersRun(t *testing.T) {
	d := newTestService(t)

	var created *models.Run
	d.repo.On("CreateRun", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		created = ret[*models.Run](args, 1)
	}).Return(nil).Once()

	run, err := d.svc.CreateRun(context.Background(), validInput())
	require.NoError(t, err)
	require.Same(t, created, run)
	require.Equal(t, models.RunStatusCreated, run.Status)
	require.Equal(t, "s3://data-lake/input/curves/", run.CurvesURI)
	require.Equal(t, "2022-08-31", run.ReportDttm.Format(reportDateFmt))
	require.NotEmpty(t, run.RunID)

	select {
	case queued := <-d.svc.queue:
		require.Equal(t, run.RunID, queued)
	default:
		t.Fatal("run was not queued for processing")
	}
}

func TestCreateRunRepositoryError(t *testing.T) {
	d := newTestService(t)

	d.repo.On("CreateRun", mock.Anything, mock.Anything).Return(errors.New("db down")).Once()

	_, err := d.svc.CreateRun(context.Background(), validInput())
	require.ErrorContains(t, err, "db down")
	require.Empty(t, d.svc.queue)
}

func TestGetRunInvalidIDIsNotFound(t *testing.T) {
	d := newTestService(t)

	_, err := d.svc.GetRun(context.Background(), "not-a-uuid")
	require.ErrorIs(t, err, models.ErrRunNotFound)
}

func TestCancelFinishedRun(t *testing.T) {
	d := newTestService(t)

	run := runningRun(models.StagePV)
	run.Status = models.RunStatusSucceeded
	d.repo.On("LockRun", mock.Anything, testRunID).Return(run, nil).Once()

	_, err := d.svc.CancelRun(context.Background(), testRunID)
	require.ErrorIs(t, err, models.ErrRunFinished)
}

func TestProcessRunLandsSplitsAndDispatches(t *testing.T) {
	d := newTestService(t)

	run := &models.Run{
		RunID:       testRunID,
		PortfolioID: testPortfolioID,
		ReportDttm:  runningRun(models.StagePrep).ReportDttm,
		SourceURI:   "s3://inbox/portfolio/deals.csv",
		CurvesURI:   "s3://inbox/portfolio/curves/",
		Status:      models.RunStatusCreated,
	}
	csv := csvHeader + "1;RUB;;;ENDDATE;\n2;RUB;;;ENDDATE;\n"

	d.repo.On("GetRun", mock.Anything, testRunID).Return(run, nil).Once()
	d.store.On("Copy", mock.Anything, "inbox", "portfolio/deals.csv", testLake, "input/"+testRunID+"/deals.csv").Return(nil).Once()
	d.store.On("CopyPrefix", mock.Anything, "inbox", "portfolio/curves/", testLake, "input/"+testRunID+"/curves/").Return(2, nil).Once()
	d.repo.On("UpdateRunStatus", mock.Anything, testRunID, models.RunStatusSplitting).Return(nil).Once()
	d.store.On("Open", mock.Anything, testLake, "input/"+testRunID+"/deals.csv").
		Return(io.NopCloser(strings.NewReader(csv)), nil).Once()
	d.store.On("Put", mock.Anything, testLake, mock.MatchedBy(func(key string) bool {
		return strings.HasSuffix(key, ".ids")
	}), []byte("1\n2\n")).Return(nil).Once()
	d.repo.On("InsertBatch", mock.Anything, mock.Anything).Return(nil).Once()

	var rc models.RunContext
	d.store.On("Put", mock.Anything, testLake, "runs/"+testRunID+"/context.json", mock.Anything).
		Run(func(args mock.Arguments) {
			data, ok := args.Get(3).([]byte)
			require.True(t, ok)
			require.NoError(t, json.Unmarshal(data, &rc))
		}).Return(nil).Once()
	d.repo.On("StartRunStages", mock.Anything, testRunID, "s3://data-lake/runs/"+testRunID+"/context.json", 2, 1).
		Return(nil).Once()
	d.repo.On("ListPendingBatchIDs", mock.Anything, testRunID, models.StagePrep).Return([]string{}, nil).Once()

	d.svc.processRun(context.Background(), testRunID)

	require.Equal(t, models.LayoutVersion, rc.LayoutVersion)
	require.Equal(t, "s3://data-lake/input/"+testRunID+"/deals.csv", rc.SourceURI)
	require.Equal(t, "s3://data-lake/prepared/"+testRunID+"/", rc.PreparedPrefix)
	require.Equal(t, 2, rc.TotalDeals)
	require.Equal(t, 1, rc.TotalBatches)
}

func TestProcessRunFailsOnDuplicateDeals(t *testing.T) {
	d := newTestService(t)

	run := &models.Run{RunID: testRunID, Status: models.RunStatusSplitting}
	csv := csvHeader + "1;RUB;;;ENDDATE;\n1;RUB;;;ENDDATE;\n"

	d.repo.On("GetRun", mock.Anything, testRunID).Return(run, nil).Once()
	d.store.On("Open", mock.Anything, testLake, "input/"+testRunID+"/deals.csv").
		Return(io.NopCloser(strings.NewReader(csv)), nil).Once()
	d.repo.On("FailRun", mock.Anything, testRunID, mock.MatchedBy(func(reason string) bool {
		return strings.Contains(reason, "duplicate deal_id: 1")
	})).Return(nil).Once()

	d.svc.processRun(context.Background(), testRunID)
}
