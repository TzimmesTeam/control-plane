package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

type Repository struct{ db *sql.DB }
type txKey struct{}
type querier interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func New(ctx context.Context, dsn string) (*Repository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: %w", err)
	}
	return &Repository{db: db}, nil
}
func (r *Repository) Close() error                   { return r.db.Close() }
func (r *Repository) Ping(ctx context.Context) error { return r.db.PingContext(ctx) }
func (r *Repository) CheckSchema(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `SELECT r.body,b.body,t.payload,t.state FROM cp_runs r LEFT JOIN cp_batches b ON b.run_id=r.id LEFT JOIN cp_tasks t ON t.run_id=r.id LIMIT 0`)
	if err != nil {
		return fmt.Errorf("apply migrations/0001_simple_state_machine.sql before starting: %w", err)
	}
	return nil
}
func (r *Repository) q(ctx context.Context) querier {
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return tx
	}
	return r.db
}
func (r *Repository) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return fn(ctx)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}
	return tx.Commit()
}
func readJSON[T any](row *sql.Row) (*T, error) {
	var raw []byte
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, models.ErrNotFound
		}
		return nil, err
	}
	var result T
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
func listJSON[T any](ctx context.Context, q querier, query string, args ...any) ([]T, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]T, 0)
	for rows.Next() {
		var raw []byte
		var item T
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}
func (r *Repository) EnsureRun(ctx context.Context, id, snapshot string) error {
	now := time.Now().UTC()
	run := models.Run{ID: id, SnapshotID: snapshot, State: models.Waiting, CreatedAt: now, UpdatedAt: now}
	body, err := json.Marshal(run)
	if err != nil {
		return err
	}
	_, err = r.q(ctx).ExecContext(ctx, `INSERT INTO cp_runs(id,snapshot_id,body) VALUES($1,$2,$3) ON CONFLICT(snapshot_id) DO NOTHING`, id, snapshot, string(body))
	return err
}
func (r *Repository) GetRun(ctx context.Context, id string) (*models.Run, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, models.ErrNotFound
	}
	return readJSON[models.Run](r.q(ctx).QueryRowContext(ctx, `SELECT body FROM cp_runs WHERE id=$1`, id))
}
func (r *Repository) LockRun(ctx context.Context, id string) (*models.Run, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, models.ErrNotFound
	}
	return readJSON[models.Run](r.q(ctx).QueryRowContext(ctx, `SELECT body FROM cp_runs WHERE id=$1 FOR UPDATE`, id))
}
func (r *Repository) SaveRun(ctx context.Context, run *models.Run) error {
	run.UpdatedAt = time.Now().UTC()
	body, err := json.Marshal(run)
	if err != nil {
		return err
	}
	_, err = r.q(ctx).ExecContext(ctx, `UPDATE cp_runs SET body=$2,updated_at=now() WHERE id=$1`, run.ID, string(body))
	return err
}
func (r *Repository) ActiveRuns(ctx context.Context, limit int) ([]models.Run, error) {
	return listJSON[models.Run](ctx, r.q(ctx), `SELECT body FROM cp_runs
 WHERE (body->>'state'='waiting' AND body->>'prepared'='true' AND coalesce(body->>'portfolio_id','')<>'')
 OR (body->>'state'='preparing' AND (body->>'planning_until')::timestamptz < now())
 OR (body->>'state'='aggregating' AND coalesce(body->>'task_id','')='')
 ORDER BY updated_at,id LIMIT $1`, limit)
}
func (r *Repository) SaveBatch(ctx context.Context, batch *models.Batch) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	_, err = r.q(ctx).ExecContext(ctx, `INSERT INTO cp_batches(run_id,id,seq,body) VALUES($1,$2,$3,$4)
 ON CONFLICT(run_id,id) DO UPDATE SET body=excluded.body`, batch.RunID, batch.ID, batch.Seq, string(body))
	return err
}
func (r *Repository) GetBatch(ctx context.Context, run, id string) (*models.Batch, error) {
	return readJSON[models.Batch](r.q(ctx).QueryRowContext(ctx, `SELECT body FROM cp_batches WHERE run_id=$1 AND id=$2`, run, id))
}
func (r *Repository) ListBatches(ctx context.Context, run string) ([]models.Batch, error) {
	return listJSON[models.Batch](ctx, r.q(ctx), `SELECT body FROM cp_batches WHERE run_id=$1 ORDER BY seq`, run)
}
func (r *Repository) RemainingBatches(ctx context.Context, run string) (int, error) {
	var count int
	err := r.q(ctx).QueryRowContext(ctx, `SELECT count(*) FROM cp_batches WHERE run_id=$1 AND body->>'stage'<>'done'`, run).Scan(&count)
	return count, err
}
