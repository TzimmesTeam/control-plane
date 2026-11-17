package postgres

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

func integrationRepo(t *testing.T) *Repository {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run PostgreSQL integration tests")
	}
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	schema := "cp_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Error(err)
		}
	})
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		dsn += " search_path=" + schema
	}
	repo, err := New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { repo.Close() })
	migration, err := os.ReadFile("../../../migrations/0001_simple_state_machine.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.db.Exec(string(migration)); err != nil {
		t.Fatal(err)
	}
	return repo
}
func TestPostgresStateAndOutbox(t *testing.T) {
	r := integrationRepo(t)
	ctx := context.Background()
	runID := uuid.NewString()
	taskID := uuid.NewString()
	if err := r.EnsureRun(ctx, runID, "snapshot"); err != nil {
		t.Fatal(err)
	}
	if err := r.EnsureRun(ctx, runID, "snapshot"); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("rollback")
	err := r.WithinTx(ctx, func(ctx context.Context) error {
		run, err := r.LockRun(ctx, runID)
		if err != nil {
			return err
		}
		run.State = models.Running
		if err := r.SaveRun(ctx, run); err != nil {
			return err
		}
		if err := r.AddTask(ctx, models.Task{ID: taskID, RunID: runID, Stage: models.Cashflow, Attempt: 1}); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if _, err := r.GetTask(ctx, taskID); !errors.Is(err, models.ErrNotFound) {
		t.Fatal("task survived rollback", err)
	}
	run, err := r.GetRun(ctx, runID)
	if err != nil || run.State != models.Waiting {
		t.Fatal("run survived rollback", err)
	}
	run.State = models.Running
	if err := r.SaveRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	task := models.Task{ID: taskID, RunID: runID, Stage: models.Cashflow, Attempt: 1}
	if err := r.AddTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	claims := make(chan []models.Task, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); tasks, err := r.ClaimTasks(ctx, 1, time.Minute); claims <- tasks; errs <- err }()
	}
	wg.Wait()
	close(claims)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	for tasks := range claims {
		count += len(tasks)
	}
	if count != 1 {
		t.Fatal("double claim", count)
	}
	if err := r.FinishTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkSent(ctx, taskID, time.Minute); err != nil {
		t.Fatal(err)
	}
	delivery, err := r.GetTask(ctx, taskID)
	if err != nil || delivery.State != "done" {
		t.Fatal("late publish acknowledgement reverted task", err)
	}
	batch := models.Batch{ID: uuid.NewString(), RunID: runID, Seq: 1, Stage: models.PV, TaskID: taskID}
	if err := r.SaveBatch(ctx, &batch); err != nil {
		t.Fatal(err)
	}
	if remaining, err := r.RemainingBatches(ctx, runID); err != nil || remaining != 1 {
		t.Fatal(remaining, err)
	}
	batch.Stage = models.Done
	if err := r.SaveBatch(ctx, &batch); err != nil {
		t.Fatal(err)
	}
	if remaining, err := r.RemainingBatches(ctx, runID); err != nil || remaining != 0 {
		t.Fatal(remaining, err)
	}
	batches, err := r.ListBatches(ctx, runID)
	if err != nil || len(batches) != 1 {
		t.Fatal(batches, err)
	}
}
