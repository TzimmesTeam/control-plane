package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

func success(task models.Task) events.Result {
	return events.Result{TaskID: task.ID, Status: "success", OutputLinks: []models.OutputLink{{URI: task.OutputPrefix + "result.parquet"}}, ReportLink: "s3://artifacts/report.pdf"}
}
func apply(t *testing.T, s *Service, task models.Task, event events.Result) {
	t.Helper()
	if err := s.HandleResult(context.Background(), task.Stage, event); err != nil {
		t.Fatal(err)
	}
}
func TestPreparedCallbackAloneStartsRunAndIsIdempotent(t *testing.T) {
	s, r, store, _ := fixture(t, 1)
	ctx := context.Background()
	first, err := s.Prepared(ctx, CreateRunInput{SnapshotID: "ab12"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Prepared(ctx, CreateRunInput{SnapshotID: "ab12"})
	if err != nil {
		t.Fatal(err)
	}
	s.Tick(ctx)
	if first.ID != second.ID || len(r.data.Runs) != 1 || store.plans != 1 {
		t.Fatal("callback did not start run exactly once")
	}
	if first.PortfolioID != "ab12" || first.ReportDate != "1970-01-01" || first.CurvesURI != "s3://curated/curve_snapshots/default/curves.parquet" || len(first.DefaultedFields) != 3 || r.data.Runs[first.ID].State != models.Running {
		t.Fatal("defaults not persisted", first)
	}
	s.Prepared(ctx, CreateRunInput{SnapshotID: "ab12"})
	s.Tick(ctx)
	if store.plans != 1 {
		t.Fatal("duplicate callback replanned snapshot")
	}
}
func TestBatchPipelineAndAggregation(t *testing.T) {
	s, r, store, pub := fixture(t, 2)
	run := start(t, s)
	cf := append([]models.Task{}, pub.tasks...)
	if len(cf) != 2 || cf[0].SourceVersion != "version-1" || cf[0].SourceETag != "etag-1" {
		t.Fatal("incomplete tasks")
	}
	apply(t, s, cf[0], success(cf[0]))
	s.Tick(context.Background())
	pv := pub.tasks[len(pub.tasks)-1]
	if pv.Stage != models.PV || pv.BatchID != cf[0].BatchID || len(pv.InputLinks) != 1 {
		t.Fatal("PV did not start independently with cashflow links")
	}
	apply(t, s, pv, success(pv))
	if r.data.Runs[run.ID].State != models.Running {
		t.Fatal("early aggregation")
	}
	apply(t, s, cf[1], success(cf[1]))
	s.Tick(context.Background())
	pv2 := pub.tasks[len(pub.tasks)-1]
	apply(t, s, pv2, success(pv2))
	if r.data.Runs[run.ID].State != models.Aggregating {
		t.Fatal("barrier did not close")
	}
	s.Tick(context.Background())
	agg := pub.tasks[len(pub.tasks)-1]
	if agg.Stage != models.Aggregate || agg.BatchesURI == "" || store.manifests != 1 {
		t.Fatal("missing aggregation manifest")
	}
	apply(t, s, agg, success(agg))
	if r.data.Runs[run.ID].State != models.Succeeded {
		t.Fatal("run not completed")
	}
}
func TestRetriesFenceLateResultsAndStopAtLimit(t *testing.T) {
	s, r, _, pub := fixture(t, 1)
	run := start(t, s)
	first := pub.tasks[0]
	task := first
	for attempt := 1; attempt <= 3; attempt++ {
		if task.Attempt != attempt {
			t.Fatal("incorrect attempt")
		}
		apply(t, s, task, events.Result{TaskID: task.ID, Status: "error", Error: "boom"})
		apply(t, s, first, success(first))
		s.Tick(context.Background())
		task = pub.tasks[len(pub.tasks)-1]
	}
	if r.data.Runs[run.ID].State != models.Failed || len(pub.tasks) != 3 {
		t.Fatal("retry limit or fencing failed")
	}
	if pub.tasks[0].OutputPrefix == pub.tasks[1].OutputPrefix {
		t.Fatal("attempts share output location")
	}
}
func TestStateAndOutboxRollbackTogether(t *testing.T) {
	s, r, _, pub := fixture(t, 1)
	start(t, s)
	task := pub.tasks[0]
	r.failAdd = true
	if err := s.HandleResult(context.Background(), task.Stage, success(task)); err == nil {
		t.Fatal("expected transaction failure")
	}
	if r.data.Tasks[task.ID].State != "sent" || r.data.Batches[task.BatchID].Stage != models.Cashflow {
		t.Fatal("partial state commit")
	}
	r.failAdd = false
	apply(t, s, task, success(task))
	if len(r.data.Tasks) != 2 {
		t.Fatal("redelivery did not recover")
	}
}
func TestDuplicateConcurrentResultsCreateOneNextTask(t *testing.T) {
	s, r, _, pub := fixture(t, 1)
	start(t, s)
	task := pub.tasks[0]
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.HandleResult(context.Background(), task.Stage, success(task)) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(r.data.Tasks) != 2 {
		t.Fatal("duplicate next tasks")
	}
}
func TestConcurrentLastPVResultsOpenOneAggregation(t *testing.T) {
	s, r, _, pub := fixture(t, 2)
	run := start(t, s)
	cf := append([]models.Task{}, pub.tasks...)
	for _, task := range cf {
		apply(t, s, task, success(task))
	}
	s.Tick(context.Background())
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, task := range pub.tasks {
		if task.Stage != models.PV {
			continue
		}
		wg.Add(1)
		go func(task models.Task) {
			defer wg.Done()
			errs <- s.HandleResult(context.Background(), task.Stage, success(task))
		}(task)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if r.data.Runs[run.ID].State != models.Aggregating {
		t.Fatal("last-batch barrier lost")
	}
	s.Tick(context.Background())
	s.Tick(context.Background())
	count := 0
	for _, task := range r.data.Tasks {
		if task.Task.Stage == models.Aggregate {
			count++
		}
	}
	if count != 1 {
		t.Fatal("aggregation created more than once", count)
	}
}
func TestExecutionTimeoutAndRenewedLease(t *testing.T) {
	s, r, _, pub := fixture(t, 1)
	start(t, s)
	task := pub.tasks[0]
	if err := s.expire(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}
	if len(r.data.Tasks) != 1 {
		t.Fatal("unexpired task retried")
	}
	d := r.data.Tasks[task.ID]
	d.LeaseUntil = time.Now().Add(-time.Minute)
	r.data.Tasks[task.ID] = d
	if err := s.expire(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}
	if len(r.data.Tasks) != 2 {
		t.Fatal("expired task not retried")
	}
	apply(t, s, task, success(task))
	if r.data.Batches[task.BatchID].Stage != models.Cashflow {
		t.Fatal("expired attempt accepted")
	}
}
func TestAmbiguousPublishFailureStillAcceptsWorkerResult(t *testing.T) {
	s, r, _, pub := fixture(t, 1)
	pub.err = errors.New("ack lost")
	start(t, s)
	task := pub.tasks[0]
	apply(t, s, task, success(task))
	if r.data.Batches[task.BatchID].Stage != models.PV {
		t.Fatal("valid result after publish timeout lost")
	}
}
func TestFastResultBeforePublishAckIsNotReverted(t *testing.T) {
	s, r, _, pub := fixture(t, 1)
	pub.onPublish = func(task models.Task) {
		if task.Stage == models.Cashflow {
			apply(t, s, task, success(task))
		}
	}
	start(t, s)
	if r.data.Tasks[pub.tasks[0].ID].State != "done" {
		t.Fatal("publish acknowledgement reverted completion")
	}
}
func TestRestartRepublishesSameTaskID(t *testing.T) {
	s, r, store, pub := fixture(t, 1)
	start(t, s)
	task := pub.tasks[0]
	d := r.data.Tasks[task.ID]
	d.State = "publishing"
	d.LeaseUntil = time.Now().Add(-time.Minute)
	r.data.Tasks[task.ID] = d
	restarted, err := NewService(s.log, r, store, pub, s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Tick(context.Background())
	if len(pub.tasks) != 2 || pub.tasks[1].ID != task.ID || pub.tasks[1].Attempt != 1 {
		t.Fatal("restart changed identity or execution attempt")
	}
}
func TestCancellationPreventsFurtherTasksAndResults(t *testing.T) {
	s, r, _, pub := fixture(t, 1)
	run := start(t, s)
	task := pub.tasks[0]
	if _, err := s.CancelRun(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	apply(t, s, task, success(task))
	s.Tick(context.Background())
	if r.data.Runs[run.ID].State != models.Canceled || len(pub.tasks) != 1 {
		t.Fatal("canceled run advanced")
	}
}
func TestInvalidResultCannotConsumeTask(t *testing.T) {
	s, r, _, pub := fixture(t, 1)
	start(t, s)
	task := pub.tasks[0]
	for _, result := range []events.Result{{TaskID: task.ID, Status: "weird"}, {TaskID: task.ID, Status: "success"}, {TaskID: "bad", Status: "error"}} {
		if err := s.HandleResult(context.Background(), task.Stage, result); !errors.Is(err, models.ErrInvalid) {
			t.Fatalf("expected invalid: %v", err)
		}
	}
	if err := s.HandleResult(context.Background(), models.PV, success(task)); !errors.Is(err, models.ErrInvalid) {
		t.Fatal("wrong subject accepted")
	}
	if r.data.Tasks[task.ID].State != "sent" {
		t.Fatal("invalid result consumed task")
	}
}
func TestEmptySnapshotAndPreparationRetryLimit(t *testing.T) {
	s, r, _, _ := fixture(t, 0)
	run := start(t, s)
	if r.data.Runs[run.ID].State != models.Failed {
		t.Fatal("empty snapshot succeeded")
	}
	s, r, store, _ := fixture(t, 1)
	store.planErr = errors.New("S3 unavailable")
	run = start(t, s)
	s.Tick(context.Background())
	s.Tick(context.Background())
	s.Tick(context.Background())
	if store.plans != 3 || r.data.Runs[run.ID].State != models.Failed {
		t.Fatal("preparation retry limit not respected")
	}
}
func TestRegistrationConflictAndValidation(t *testing.T) {
	s, r, _, _ := fixture(t, 1)
	register(t, s)
	_, err := s.CreateRun(context.Background(), CreateRunInput{SnapshotID: "ab12", PortfolioID: "other", ReportDate: "2026-11-16", CurvesURI: "s3://curated/curves.parquet"})
	if !errors.Is(err, models.ErrConflict) || len(r.data.Runs) != 1 {
		t.Fatal("conflicting snapshot changed")
	}
	if _, err := s.Prepared(context.Background(), CreateRunInput{SnapshotID: "../bad"}); !errors.Is(err, models.ErrInvalid) {
		t.Fatal("unsafe snapshot accepted")
	}
	if err := s.HandleResult(context.Background(), models.Cashflow, events.Result{TaskID: uuid.NewString(), Status: "error"}); err != nil {
		t.Fatal("unknown task should be ignored")
	}
}
