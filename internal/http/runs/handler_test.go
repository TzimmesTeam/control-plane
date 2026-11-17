package runs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/service"
)

type fakeService struct {
	snapshot string
	input    service.CreateRunInput
	err      error
}

func (f *fakeService) CreateRun(_ context.Context, in service.CreateRunInput) (*models.Run, error) {
	f.input = in
	return &models.Run{ID: "run", PlanningToken: "private-implementation-token"}, f.err
}
func (f *fakeService) Prepared(_ context.Context, in service.CreateRunInput) (*models.Run, error) {
	f.snapshot = in.SnapshotID
	f.input = in
	return &models.Run{ID: "run"}, f.err
}
func (f *fakeService) GetRun(context.Context, string) (*models.Run, error) { return nil, f.err }
func (f *fakeService) ListRunBatches(context.Context, string) ([]models.Batch, error) {
	return []models.Batch{}, f.err
}
func (f *fakeService) CancelRun(context.Context, string) (*models.Run, error) {
	return &models.Run{State: models.Canceled}, f.err
}
func request(t *testing.T, svc *fakeService, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.ReleaseMode)
	mux := gin.New()
	NewHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), svc).Register(mux)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
	return response
}
func TestPreparedCallbackAndRegistration(t *testing.T) {
	svc := &fakeService{}
	got := request(t, svc, "POST", "/snapshots/prepared", `{"snapshot_id":"ab12"}`)
	if got.Code != 200 || svc.snapshot != "ab12" {
		t.Fatal(got.Code, got.Body.String())
	}
	got = request(t, svc, "POST", "/v1/runs", `{"snapshot_id":"ab12","portfolio_id":"p1","report_dttm":"2026-11-16","curves_uri":"s3://curated/curves.parquet"}`)
	if got.Code != 200 || svc.input.SnapshotID != "ab12" || strings.Contains(got.Body.String(), "planning_token") {
		t.Fatal(got.Code, got.Body.String())
	}
}
func TestStrictBoundedJSON(t *testing.T) {
	for _, body := range []string{`{`, `{"snapshot_id":"ab12","other":1}`, `{"snapshot_id":"ab12"} {}`, `{"snapshot_id":"` + strings.Repeat("a", 1<<20) + `"}`} {
		if got := request(t, &fakeService{}, "POST", "/snapshots/prepared", body); got.Code != 400 {
			t.Fatal("invalid body accepted", got.Code)
		}
	}
}
func TestPreparedCallbackPassesOptionalMetadata(t *testing.T) {
	svc := &fakeService{}
	got := request(t, svc, "POST", "/snapshots/prepared", `{"snapshot_id":"ab12","portfolio_id":"p1","report_dttm":"2026-11-17","curves_uri":"s3://curated/curves.parquet"}`)
	if got.Code != 200 || svc.input.PortfolioID != "p1" || svc.input.ReportDate != "2026-11-17" || svc.input.CurvesURI != "s3://curated/curves.parquet" {
		t.Fatal(got.Code, svc.input)
	}
}
func TestErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{{models.ErrNotFound, 404}, {models.ErrConflict, 409}, {models.ErrInvalid, 400}, {errors.New("database password must not leak"), 500}} {
		got := request(t, &fakeService{err: tc.err}, "GET", "/v1/runs/run", "")
		if got.Code != tc.status || strings.Contains(got.Body.String(), "password") {
			t.Fatal(got.Code, got.Body.String())
		}
	}
}
