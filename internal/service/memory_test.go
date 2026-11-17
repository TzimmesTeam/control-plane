package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

type memoryState struct {
	Runs    map[string]models.Run
	Batches map[string]models.Batch
	Tasks   map[string]models.Delivery
}
type memoryRepo struct {
	mu      sync.Mutex
	data    memoryState
	failAdd bool
}

func clone[T any](in T) T {
	raw, _ := json.Marshal(in)
	var out T
	_ = json.Unmarshal(raw, &out)
	return out
}
func newMemory() *memoryRepo {
	return &memoryRepo{data: memoryState{Runs: map[string]models.Run{}, Batches: map[string]models.Batch{}, Tasks: map[string]models.Delivery{}}}
}
func (r *memoryRepo) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	before := clone(r.data)
	if err := fn(ctx); err != nil {
		r.data = before
		return err
	}
	return nil
}
func (r *memoryRepo) EnsureRun(ctx context.Context, id, snapshot string) error {
	if _, ok := r.data.Runs[id]; !ok {
		r.data.Runs[id] = models.Run{ID: id, SnapshotID: snapshot, State: models.Waiting}
	}
	return nil
}
func (r *memoryRepo) GetRun(ctx context.Context, id string) (*models.Run, error) {
	v, ok := r.data.Runs[id]
	if !ok {
		return nil, models.ErrNotFound
	}
	v = clone(v)
	return &v, nil
}
func (r *memoryRepo) LockRun(ctx context.Context, id string) (*models.Run, error) {
	return r.GetRun(ctx, id)
}
func (r *memoryRepo) SaveRun(ctx context.Context, v *models.Run) error {
	r.data.Runs[v.ID] = clone(*v)
	return nil
}
func (r *memoryRepo) ActiveRuns(ctx context.Context, limit int) ([]models.Run, error) {
	out := []models.Run{}
	for _, v := range r.data.Runs {
		if !v.State.Terminal() {
			out = append(out, clone(v))
		}
	}
	return out, nil
}
func (r *memoryRepo) SaveBatch(ctx context.Context, v *models.Batch) error {
	r.data.Batches[v.ID] = clone(*v)
	return nil
}
func (r *memoryRepo) GetBatch(ctx context.Context, run, id string) (*models.Batch, error) {
	v, ok := r.data.Batches[id]
	if !ok || v.RunID != run {
		return nil, models.ErrNotFound
	}
	v = clone(v)
	return &v, nil
}
func (r *memoryRepo) ListBatches(ctx context.Context, run string) ([]models.Batch, error) {
	out := []models.Batch{}
	for _, v := range r.data.Batches {
		if v.RunID == run {
			out = append(out, clone(v))
		}
	}
	return out, nil
}
func (r *memoryRepo) RemainingBatches(ctx context.Context, run string) (int, error) {
	count := 0
	for _, v := range r.data.Batches {
		if v.RunID == run && v.Stage != models.Done {
			count++
		}
	}
	return count, nil
}
func (r *memoryRepo) AddTask(ctx context.Context, v models.Task) error {
	if r.failAdd {
		return errors.New("DB unavailable")
	}
	if _, ok := r.data.Tasks[v.ID]; !ok {
		r.data.Tasks[v.ID] = models.Delivery{Task: clone(v), State: "pending"}
	}
	return nil
}
func (r *memoryRepo) GetTask(ctx context.Context, id string) (*models.Delivery, error) {
	v, ok := r.data.Tasks[id]
	if !ok {
		return nil, models.ErrNotFound
	}
	v = clone(v)
	return &v, nil
}
func (r *memoryRepo) FinishTask(ctx context.Context, id string) error {
	v := r.data.Tasks[id]
	v.State = "done"
	r.data.Tasks[id] = v
	return nil
}
func (r *memoryRepo) ClaimTasks(ctx context.Context, limit int, lease time.Duration) ([]models.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []models.Task{}
	for id, v := range r.data.Tasks {
		run := r.data.Runs[v.Task.RunID]
		if run.State.Terminal() {
			continue
		}
		if v.State == "pending" || v.State == "publishing" && time.Now().After(v.LeaseUntil) {
			v.State = "publishing"
			v.LeaseUntil = time.Now().Add(lease)
			r.data.Tasks[id] = v
			out = append(out, clone(v.Task))
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}
func (r *memoryRepo) MarkSent(ctx context.Context, id string, lease time.Duration) error {
	v := r.data.Tasks[id]
	if v.State == "publishing" {
		v.State = "sent"
		v.LeaseUntil = time.Now().Add(lease)
		r.data.Tasks[id] = v
	}
	return nil
}
func (r *memoryRepo) ReleaseTask(ctx context.Context, id string) error {
	v := r.data.Tasks[id]
	if v.State == "publishing" {
		v.State = "pending"
		r.data.Tasks[id] = v
	}
	return nil
}
func (r *memoryRepo) ExpiredTasks(ctx context.Context, limit int) ([]models.Task, error) {
	out := []models.Task{}
	for _, v := range r.data.Tasks {
		if v.State == "sent" && time.Now().After(v.LeaseUntil) {
			out = append(out, clone(v.Task))
		}
	}
	return out, nil
}

type fakeStore struct {
	count     int
	planErr   error
	plans     int
	manifests int
}

func (f *fakeStore) Resolve(context.Context, string) (string, error) {
	return "s3://curated/source=x/ingest_date=2026-11-16/snapshot_id=ab12/deals.parquet", nil
}
func (f *fakeStore) Plan(ctx context.Context, run *models.Run, size, max int) ([]models.Batch, string, string, error) {
	f.plans++
	if f.planErr != nil {
		return nil, "", "", f.planErr
	}
	out := []models.Batch{}
	for i := 0; i < f.count; i++ {
		out = append(out, models.Batch{ID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(run.ID+string(rune(i)))).String(), RunID: run.ID, Seq: i + 1, DealCount: 2, IDsURI: "s3://artifacts/ids.parquet"})
	}
	return out, "version-1", "etag-1", nil
}
func (f *fakeStore) PutJSON(context.Context, string, any) error { f.manifests++; return nil }

type fakePublisher struct {
	tasks     []models.Task
	err       error
	onPublish func(models.Task)
}

func (f *fakePublisher) PublishTask(ctx context.Context, subject, id string, value any) error {
	task := value.(models.Task)
	f.tasks = append(f.tasks, clone(task))
	if f.onPublish != nil {
		f.onPublish(task)
	}
	return f.err
}

func fixture(t *testing.T, count int) (*Service, *memoryRepo, *fakeStore, *fakePublisher) {
	t.Helper()
	repo := newMemory()
	store := &fakeStore{count: count}
	pub := &fakePublisher{}
	svc, err := NewService(slog.New(slog.NewTextHandler(io.Discard, nil)), repo, store, pub, Settings{BatchSize: 2, MaxBatches: 100, MaxAttempts: 3, ScanInterval: time.Second, LeaseTTL: time.Minute, PublishTimeout: time.Second, ArtifactsBucket: "artifacts", CuratedBucket: "curated", TaskSubjects: map[models.Stage]string{models.Cashflow: "cf", models.PV: "pv", models.Aggregate: "agg"}})
	if err != nil {
		t.Fatal(err)
	}
	return svc, repo, store, pub
}
func register(t *testing.T, s *Service) *models.Run {
	t.Helper()
	run, err := s.CreateRun(context.Background(), CreateRunInput{SnapshotID: "ab12", PortfolioID: "portfolio-1", ReportDate: "2026-11-16", CurvesURI: "s3://curated/curves.parquet"})
	if err != nil {
		t.Fatal(err)
	}
	return run
}
func start(t *testing.T, s *Service) *models.Run {
	t.Helper()
	run := register(t, s)
	if _, err := s.Prepared(context.Background(), CreateRunInput{SnapshotID: "ab12"}); err != nil {
		t.Fatal(err)
	}
	s.Tick(context.Background())
	return run
}
