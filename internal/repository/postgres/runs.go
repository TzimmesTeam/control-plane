package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

const selectRunSQL = `
	SELECT
		run_id::text,
		portfolio_id::text,
		report_dttm,
		source_uri,
		curves_uri,
		COALESCE(context_uri, ''),
		status::text,
		COALESCE(current_stage::text, ''),
		total_deals,
		total_batches,
		COALESCE(report_link, ''),
		COALESCE(error, ''),
		created_at,
		updated_at,
		finished_at
	FROM runs
`

func (r *Repository) CreateRun(ctx context.Context, run *models.Run) error {
	const op = "Repository.CreateRun"

	if run == nil {
		return fmt.Errorf("%s: run is nil", op)
	}

	err := r.q(ctx).QueryRowContext(ctx, `
		INSERT INTO runs (
			run_id,
			portfolio_id,
			report_dttm,
			source_uri,
			curves_uri,
			status
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6::run_status)
		RETURNING created_at, updated_at
	`, run.RunID, run.PortfolioID, run.ReportDttm, run.SourceURI, run.CurvesURI, string(run.Status)).Scan(
		&run.CreatedAt,
		&run.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("%s: insert run: %w", op, err)
	}

	return nil
}

func (r *Repository) GetRun(ctx context.Context, runID string) (*models.Run, error) {
	const op = "Repository.GetRun"

	run, err := scanRun(r.q(ctx).QueryRowContext(ctx, selectRunSQL+`WHERE run_id = $1::uuid`, runID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%s: %w", op, models.ErrRunNotFound)
		}
		return nil, fmt.Errorf("%s: select run: %w", op, err)
	}

	return run, nil
}

func (r *Repository) LockRun(ctx context.Context, runID string) (*models.Run, error) {
	const op = "Repository.LockRun"

	if _, ok := ctx.Value(txKey{}).(*sql.Tx); !ok {
		return nil, fmt.Errorf("%s: must be called inside a transaction", op)
	}

	run, err := scanRun(r.q(ctx).QueryRowContext(ctx, selectRunSQL+`WHERE run_id = $1::uuid FOR UPDATE`, runID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%s: %w", op, models.ErrRunNotFound)
		}
		return nil, fmt.Errorf("%s: select run for update: %w", op, err)
	}

	return run, nil
}

func (r *Repository) ListActiveRuns(ctx context.Context) ([]*models.Run, error) {
	return queryList(ctx, r, "Repository.ListActiveRuns", scanRun, selectRunSQL+`
		WHERE status IN ('created', 'splitting', 'running', 'aggregating')
		ORDER BY created_at
	`)
}

func (r *Repository) UpdateRunStatus(ctx context.Context, runID string, status models.RunStatus) error {
	const op = "Repository.UpdateRunStatus"

	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE runs
		SET status = $2::run_status,
			updated_at = now()
		WHERE run_id = $1::uuid
	`, runID, string(status))
	if err != nil {
		return fmt.Errorf("%s: update run: %w", op, err)
	}

	return requireAffected(res, op, models.ErrRunNotFound)
}

func (r *Repository) UpdateRunStage(ctx context.Context, runID string, status models.RunStatus, stage models.Stage) error {
	const op = "Repository.UpdateRunStage"

	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE runs
		SET status = $2::run_status,
			current_stage = $3::stage_name,
			updated_at = now()
		WHERE run_id = $1::uuid
	`, runID, string(status), string(stage))
	if err != nil {
		return fmt.Errorf("%s: update run: %w", op, err)
	}

	return requireAffected(res, op, models.ErrRunNotFound)
}

func (r *Repository) StartRunStages(ctx context.Context, runID, contextURI string, totalDeals, totalBatches int) error {
	const op = "Repository.StartRunStages"

	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE runs
		SET status = 'running',
			current_stage = 'prep',
			context_uri = $2,
			total_deals = $3,
			total_batches = $4,
			updated_at = now()
		WHERE run_id = $1::uuid
		  AND status = 'splitting'
	`, runID, contextURI, totalDeals, totalBatches)
	if err != nil {
		return fmt.Errorf("%s: update run: %w", op, err)
	}

	return requireAffected(res, op, models.ErrStateConflict)
}

func (r *Repository) FailRun(ctx context.Context, runID, reason string) error {
	const op = "Repository.FailRun"

	_, err := r.q(ctx).ExecContext(ctx, `
		UPDATE runs
		SET status = 'failed',
			error = $2,
			finished_at = now(),
			updated_at = now()
		WHERE run_id = $1::uuid
		  AND status NOT IN ('succeeded', 'failed', 'canceled')
	`, runID, reason)
	if err != nil {
		return fmt.Errorf("%s: update run: %w", op, err)
	}

	return nil
}

func (r *Repository) CompleteRun(ctx context.Context, runID, reportLink string) error {
	const op = "Repository.CompleteRun"

	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE runs
		SET status = 'succeeded',
			report_link = $2,
			finished_at = now(),
			updated_at = now()
		WHERE run_id = $1::uuid
		  AND status = 'aggregating'
	`, runID, reportLink)
	if err != nil {
		return fmt.Errorf("%s: update run: %w", op, err)
	}

	return requireAffected(res, op, models.ErrStateConflict)
}

func (r *Repository) CancelRun(ctx context.Context, runID string) error {
	const op = "Repository.CancelRun"

	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE runs
		SET status = 'canceled',
			finished_at = now(),
			updated_at = now()
		WHERE run_id = $1::uuid
		  AND status NOT IN ('succeeded', 'failed', 'canceled')
	`, runID)
	if err != nil {
		return fmt.Errorf("%s: update run: %w", op, err)
	}

	return requireAffected(res, op, models.ErrRunFinished)
}
