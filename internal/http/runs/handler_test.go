package runs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const testRunID = "11111111-1111-4111-8111-111111111111"

type runServiceMock struct{ mock.Mock }

func ret[T any](args mock.Arguments, i int) T {
	v, ok := args.Get(i).(T)
	if !ok {
		var zero T
		return zero
	}

	return v
}

func (m *runServiceMock) CreateRun(ctx context.Context, in service.CreateRunInput) (*models.Run, error) {
	args := m.Called(ctx, in)
	return ret[*models.Run](args, 0), args.Error(1)
}

func (m *runServiceMock) GetRun(ctx context.Context, runID string) (*models.RunDetails, error) {
	args := m.Called(ctx, runID)
	return ret[*models.RunDetails](args, 0), args.Error(1)
}

func (m *runServiceMock) ListRunBatches(ctx context.Context, runID string, filter models.BatchStageFilter) ([]*models.BatchDetails, error) {
	args := m.Called(ctx, runID, filter)
	return ret[[]*models.BatchDetails](args, 0), args.Error(1)
}

func (m *runServiceMock) CancelRun(ctx context.Context, runID string) (*models.RunDetails, error) {
	args := m.Called(ctx, runID)
	return ret[*models.RunDetails](args, 0), args.Error(1)
}

func newTestRouter(svc RunService) *gin.Engine {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	NewHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), svc).Register(engine)
	return engine
}

func serve(t *testing.T, router http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	return rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) ErrorBody {
	t.Helper()

	var resp ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	return resp.Error
}

func TestCreateRunAccepted(t *testing.T) {
	svc := &runServiceMock{}
	in := service.CreateRunInput{
		PortfolioID: "99999999-9999-4999-8999-999999999999",
		ReportDttm:  "2022-08-31",
		SourceURI:   "s3://data-lake/input/deals.csv",
		CurvesURI:   "s3://data-lake/input/curves/",
	}
	svc.On("CreateRun", mock.Anything, in).
		Return(&models.Run{RunID: testRunID, Status: models.RunStatusCreated}, nil).Once()

	body := `{"portfolio_id":"99999999-9999-4999-8999-999999999999","report_dttm":"2022-08-31",` +
		`"source_uri":"s3://data-lake/input/deals.csv","curves_uri":"s3://data-lake/input/curves/"}`
	rec := serve(t, newTestRouter(svc), http.MethodPost, "/v1/runs", body)

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.JSONEq(t, `{"run_id":"`+testRunID+`","status":"created"}`, rec.Body.String())
	svc.AssertExpectations(t)
}

func TestCreateRunBadRequest(t *testing.T) {
	tests := []struct {
		name string
		body string
		err  error
	}{
		{name: "invalid json", body: `{`},
		{name: "unknown field", body: `{"foo":1}`},
		{name: "validation error", body: `{}`, err: &service.ValidationError{Field: "portfolio_id", Reason: "must be a UUID"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &runServiceMock{}
			if tt.err != nil {
				svc.On("CreateRun", mock.Anything, mock.Anything).Return(nil, tt.err).Once()
			}

			rec := serve(t, newTestRouter(svc), http.MethodPost, "/v1/runs", tt.body)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Equal(t, codeInvalidRequest, decodeError(t, rec).Code)
		})
	}
}

func TestGetRunNotFound(t *testing.T) {
	svc := &runServiceMock{}
	svc.On("GetRun", mock.Anything, testRunID).Return(nil, models.ErrRunNotFound).Once()

	rec := serve(t, newTestRouter(svc), http.MethodGet, "/v1/runs/"+testRunID, "")

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, codeRunNotFound, decodeError(t, rec).Code)
}

func TestGetRunInternalError(t *testing.T) {
	svc := &runServiceMock{}
	svc.On("GetRun", mock.Anything, testRunID).Return(nil, errors.New("db down")).Once()

	rec := serve(t, newTestRouter(svc), http.MethodGet, "/v1/runs/"+testRunID, "")

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, codeInternal, decodeError(t, rec).Code)
}

func TestGetRunMapsResponse(t *testing.T) {
	created := time.Date(2026, 11, 6, 19, 0, 0, 0, time.UTC)
	svc := &runServiceMock{}
	svc.On("GetRun", mock.Anything, testRunID).Return(&models.RunDetails{
		Run: &models.Run{
			RunID:        testRunID,
			PortfolioID:  "99999999-9999-4999-8999-999999999999",
			ReportDttm:   time.Date(2022, 8, 31, 0, 0, 0, 0, time.UTC),
			Status:       models.RunStatusRunning,
			CurrentStage: models.StageCashflow,
			TotalDeals:   55,
			TotalBatches: 2,
			CreatedAt:    created,
		},
		Stages: []models.StageProgress{
			{Stage: models.StagePrep, Total: 2, Done: 2},
			{Stage: models.StageCashflow, Total: 2, Done: 1},
			{Stage: models.StagePV, Total: 2},
		},
	}, nil).Once()

	rec := serve(t, newTestRouter(svc), http.MethodGet, "/v1/runs/"+testRunID, "")

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{
		"run_id": "`+testRunID+`",
		"portfolio_id": "99999999-9999-4999-8999-999999999999",
		"report_dttm": "2022-08-31",
		"status": "running",
		"current_stage": "cashflow",
		"total_deals": 55,
		"total_batches": 2,
		"stages": [
			{"stage": "prep", "total": 2, "done": 2, "failed": 0},
			{"stage": "cashflow", "total": 2, "done": 1, "failed": 0},
			{"stage": "pv", "total": 2, "done": 0, "failed": 0}
		],
		"report_link": null,
		"error": null,
		"created_at": "2026-11-06T19:00:00Z",
		"finished_at": null
	}`, rec.Body.String())
}

func TestListBatchesPassesFilter(t *testing.T) {
	svc := &runServiceMock{}
	filter := models.BatchStageFilter{Stage: models.StagePrep, Status: models.BatchStatusDone}
	svc.On("ListRunBatches", mock.Anything, testRunID, filter).Return([]*models.BatchDetails{{
		Batch: &models.Batch{BatchID: "b1", Seq: 1, Currency: "RUB", DealCount: 30, Weight: 1450},
		Stages: []*models.BatchStage{{
			Stage:       models.StagePrep,
			Status:      models.BatchStatusDone,
			Attempt:     1,
			Stats:       map[string]int64{"deals_in": 30},
			OutputLinks: []models.OutputLink{{URI: "s3://x"}},
		}},
	}}, nil).Once()

	rec := serve(t, newTestRouter(svc), http.MethodGet, "/v1/runs/"+testRunID+"/batches?stage=prep&status=done", "")

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"batches":[{"batch_id":"b1","seq":1,"currency":"RUB","deal_count":30,"weight":1450,
		"stages":[{"stage":"prep","status":"done","attempt":1,"stats":{"deals_in":30},"output_links":[{"uri":"s3://x"}]}]}]}`,
		rec.Body.String())
}

func TestCancelFinishedRunConflict(t *testing.T) {
	svc := &runServiceMock{}
	svc.On("CancelRun", mock.Anything, testRunID).Return(nil, models.ErrRunFinished).Once()

	rec := serve(t, newTestRouter(svc), http.MethodPost, "/v1/runs/"+testRunID+"/cancel", "")

	require.Equal(t, http.StatusConflict, rec.Code)
	require.Equal(t, codeRunFinished, decodeError(t, rec).Code)
}
