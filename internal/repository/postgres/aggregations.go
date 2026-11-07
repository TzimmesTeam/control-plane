package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

const selectAggregationSQL = `
	SELECT
		run_id::text,
		portfolio_id::text,
		status::text,
		attempt,
		COALESCE(batches_uri, ''),
		COALESCE(report_link, ''),
		stats,
		COALESCE(error, ''),
		dispatched_at,
		done_at,
		lease_until
	FROM aggregations
`

func (r *Repository) EnsureAggregation(ctx context.Context, runID, portfolioID string) error {
	const op = "Repository.EnsureAggregation"

	_, err := r.q(ctx).ExecContext(ctx, `
		INSERT INTO aggregations (run_id, portfolio_id)
		VALUES ($1::uuid, $2::uuid)
		ON CONFLICT DO NOTHING
	`, runID, portfolioID)
	if err != nil {
		return fmt.Errorf("%s: insert aggregation: %w", op, err)
	}

	return nil
}

func (r *Repository) GetAggregation(ctx context.Context, runID string) (*models.Aggregation, error) {
	const op = "Repository.GetAggregation"

	agg, err := scanAggregation(r.q(ctx).QueryRowContext(ctx, selectAggregationSQL+`WHERE run_id = $1::uuid`, runID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%s: %w", op, models.ErrAggregationNotFound)
		}
		return nil, fmt.Errorf("%s: select aggregation: %w", op, err)
	}

	return agg, nil
}

func (r *Repository) MarkAggregationDispatched(
	ctx context.Context,
	runID string,
	attempt int,
	lease time.Duration,
	batchesURI string,
) error {
	const op = "Repository.MarkAggregationDispatched"

	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE aggregations
		SET status = 'dispatched',
			attempt = $2,
			batches_uri = $4,
			dispatched_at = now(),
			lease_until = now() + $3 * interval '1 millisecond'
		WHERE run_id = $1::uuid
		  AND status = 'pending'
	`, runID, attempt, lease.Milliseconds(), batchesURI)
	if err != nil {
		return fmt.Errorf("%s: update aggregation: %w", op, err)
	}

	return requireAffected(res, op, models.ErrStateConflict)
}

func (r *Repository) MarkAggregationDone(ctx context.Context, runID, reportLink string, stats map[string]int64) error {
	const op = "Repository.MarkAggregationDone"

	_, statsJSON, err := marshalOutputs(nil, stats)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE aggregations
		SET status = 'done',
			report_link = $2,
			stats = $3::jsonb,
			error = NULL,
			done_at = now(),
			lease_until = NULL
		WHERE run_id = $1::uuid
	`, runID, reportLink, statsJSON)
	if err != nil {
		return fmt.Errorf("%s: update aggregation: %w", op, err)
	}

	return requireAffected(res, op, models.ErrAggregationNotFound)
}

func (r *Repository) MarkAggregationRetry(ctx context.Context, runID, reason string) error {
	const op = "Repository.MarkAggregationRetry"

	return r.setAggregationStatus(ctx, op, runID, models.BatchStatusPending, reason)
}

func (r *Repository) MarkAggregationDead(ctx context.Context, runID, reason string) error {
	const op = "Repository.MarkAggregationDead"

	return r.setAggregationStatus(ctx, op, runID, models.BatchStatusDead, reason)
}

func (r *Repository) setAggregationStatus(ctx context.Context, op, runID string, status models.BatchStatus, reason string) error {
	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE aggregations
		SET status = $2::batch_status,
			error = $3,
			lease_until = NULL
		WHERE run_id = $1::uuid
	`, runID, string(status), reason)
	if err != nil {
		return fmt.Errorf("%s: update aggregation: %w", op, err)
	}

	return requireAffected(res, op, models.ErrAggregationNotFound)
}

func (r *Repository) ListExpiredAggregations(ctx context.Context, limit int) ([]*models.Aggregation, error) {
	return queryList(ctx, r, "Repository.ListExpiredAggregations", scanAggregation, selectAggregationSQL+`
		WHERE status = 'dispatched'
		  AND lease_until < now()
		ORDER BY lease_until
		LIMIT $1
	`, limit)
}
