package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

func (r *Repository) AddTask(ctx context.Context, task models.Task) error {
	body, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = r.q(ctx).ExecContext(ctx, `INSERT INTO cp_tasks(id,run_id,payload) VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING`, task.ID, task.RunID, string(body))
	return err
}
func (r *Repository) GetTask(ctx context.Context, id string) (*models.Delivery, error) {
	var result models.Delivery
	var raw []byte
	var lease sql.NullTime
	err := r.q(ctx).QueryRowContext(ctx, `SELECT payload,state,lease_until FROM cp_tasks WHERE id=$1`, id).Scan(&raw, &result.State, &lease)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, models.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &result.Task); err != nil {
		return nil, err
	}
	result.LeaseUntil = lease.Time
	return &result, nil
}
func (r *Repository) FinishTask(ctx context.Context, id string) error {
	_, err := r.q(ctx).ExecContext(ctx, `UPDATE cp_tasks SET state='done',lease_until=NULL WHERE id=$1`, id)
	return err
}
func (r *Repository) ClaimTasks(ctx context.Context, limit int, lease time.Duration) ([]models.Task, error) {
	return listJSON[models.Task](ctx, r.q(ctx), `UPDATE cp_tasks SET state='publishing',lease_until=now()+$2::double precision*interval '1 second'
 WHERE id IN (SELECT t.id FROM cp_tasks t JOIN cp_runs r ON r.id=t.run_id
 WHERE r.body->>'state' IN ('running','aggregating') AND
 ((t.state='pending' AND t.available_at<=now()) OR (t.state='publishing' AND t.lease_until<now()))
 ORDER BY t.available_at,t.id LIMIT $1 FOR UPDATE OF t SKIP LOCKED) RETURNING payload`, limit, lease.Seconds())
}
func (r *Repository) MarkSent(ctx context.Context, id string, lease time.Duration) error {
	// A fast result can finish the task before the publish acknowledgement.
	_, err := r.q(ctx).ExecContext(ctx, `UPDATE cp_tasks SET state='sent',lease_until=now()+$2::double precision*interval '1 second' WHERE id=$1 AND state='publishing'`, id, lease.Seconds())
	return err
}
func (r *Repository) ReleaseTask(ctx context.Context, id string) error {
	_, err := r.q(ctx).ExecContext(ctx, `UPDATE cp_tasks SET state='pending',lease_until=NULL,available_at=now()+interval '2 seconds' WHERE id=$1 AND state='publishing'`, id)
	return err
}
func (r *Repository) ExpiredTasks(ctx context.Context, limit int) ([]models.Task, error) {
	return listJSON[models.Task](ctx, r.q(ctx), `SELECT t.payload FROM cp_tasks t JOIN cp_runs r ON r.id=t.run_id
 WHERE t.state='sent' AND t.lease_until<now() AND r.body->>'state' IN ('running','aggregating') ORDER BY t.lease_until LIMIT $1`, limit)
}
