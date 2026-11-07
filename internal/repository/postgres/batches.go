package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/lib/pq"
)

const selectBatchStageSQL = `
	SELECT
		bs.run_id::text,
		bs.batch_id::text,
		bs.stage::text,
		bs.status::text,
		bs.attempt,
		bs.output_links,
		bs.stats,
		COALESCE(bs.error, ''),
		bs.dispatched_at,
		bs.done_at,
		bs.lease_until,
		b.deal_count
	FROM batch_stages bs
	JOIN batches b ON b.run_id = bs.run_id AND b.batch_id = bs.batch_id
`

func (r *Repository) InsertBatch(ctx context.Context, batch *models.Batch) error {
	const op = "Repository.InsertBatch"

	if batch == nil {
		return fmt.Errorf("%s: batch is nil", op)
	}

	return r.WithinTx(ctx, func(ctx context.Context) error {
		_, err := r.q(ctx).ExecContext(ctx, `
			INSERT INTO batches (
				run_id,
				batch_id,
				seq,
				ids_uri,
				deal_count,
				weight,
				currency
			)
			VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7)
			ON CONFLICT DO NOTHING
		`, batch.RunID, batch.BatchID, batch.Seq, batch.IdsURI, batch.DealCount, batch.Weight, batch.Currency)
		if err != nil {
			return fmt.Errorf("%s: insert batch: %w", op, err)
		}

		_, err = r.q(ctx).ExecContext(ctx, `
			INSERT INTO batch_stages (run_id, batch_id, stage)
			SELECT $1::uuid, $2::uuid, s
			FROM unnest($3::stage_name[]) AS s
			ON CONFLICT DO NOTHING
		`, batch.RunID, batch.BatchID, stageArray(models.BatchStages()))
		if err != nil {
			return fmt.Errorf("%s: insert batch stages: %w", op, err)
		}

		return nil
	})
}

func (r *Repository) ListBatches(ctx context.Context, runID string) ([]*models.Batch, error) {
	return queryList(ctx, r, "Repository.ListBatches", scanBatch, `
		SELECT
			run_id::text,
			batch_id::text,
			seq,
			ids_uri,
			deal_count,
			weight,
			currency,
			created_at
		FROM batches
		WHERE run_id = $1::uuid
		ORDER BY seq
	`, runID)
}

func (r *Repository) ListBatchStages(ctx context.Context, runID string, filter models.BatchStageFilter) ([]*models.BatchStage, error) {
	return queryList(ctx, r, "Repository.ListBatchStages", scanBatchStage, selectBatchStageSQL+`
		WHERE bs.run_id = $1::uuid
		  AND ($2::text = '' OR bs.stage::text = $2::text)
		  AND ($3::text = '' OR bs.status::text = $3::text)
		ORDER BY b.seq, bs.stage
	`, runID, string(filter.Stage), string(filter.Status))
}

func (r *Repository) GetBatchStage(ctx context.Context, runID, batchID string, stage models.Stage) (*models.BatchStage, error) {
	const op = "Repository.GetBatchStage"

	bs, err := scanBatchStage(r.q(ctx).QueryRowContext(ctx, selectBatchStageSQL+`
		WHERE bs.run_id = $1::uuid
		  AND bs.batch_id = $2::uuid
		  AND bs.stage = $3::stage_name
	`, runID, batchID, string(stage)))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%s: %w", op, models.ErrBatchNotFound)
		}
		return nil, fmt.Errorf("%s: select batch stage: %w", op, err)
	}

	return bs, nil
}

func (r *Repository) ListPendingBatchIDs(ctx context.Context, runID string, stage models.Stage) ([]string, error) {
	return queryList(ctx, r, "Repository.ListPendingBatchIDs", scanString, `
		SELECT bs.batch_id::text
		FROM batch_stages bs
		JOIN batches b ON b.run_id = bs.run_id AND b.batch_id = bs.batch_id
		WHERE bs.run_id = $1::uuid
		  AND bs.stage = $2::stage_name
		  AND bs.status = 'pending'
		ORDER BY b.seq
	`, runID, string(stage))
}

func (r *Repository) MarkBatchDispatched(
	ctx context.Context,
	runID, batchID string,
	stage models.Stage,
	attempt int,
	lease time.Duration,
) error {
	const op = "Repository.MarkBatchDispatched"

	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE batch_stages
		SET status = 'dispatched',
			attempt = $4,
			dispatched_at = now(),
			lease_until = now() + $5 * interval '1 millisecond'
		WHERE run_id = $1::uuid
		  AND batch_id = $2::uuid
		  AND stage = $3::stage_name
		  AND status = 'pending'
	`, runID, batchID, string(stage), attempt, lease.Milliseconds())
	if err != nil {
		return fmt.Errorf("%s: update batch stage: %w", op, err)
	}

	return requireAffected(res, op, models.ErrStateConflict)
}

func (r *Repository) MarkBatchDone(
	ctx context.Context,
	runID, batchID string,
	stage models.Stage,
	links []models.OutputLink,
	stats map[string]int64,
) error {
	const op = "Repository.MarkBatchDone"

	linksJSON, statsJSON, err := marshalOutputs(links, stats)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE batch_stages
		SET status = 'done',
			output_links = $4::jsonb,
			stats = $5::jsonb,
			error = NULL,
			done_at = now(),
			lease_until = NULL
		WHERE run_id = $1::uuid
		  AND batch_id = $2::uuid
		  AND stage = $3::stage_name
	`, runID, batchID, string(stage), linksJSON, statsJSON)
	if err != nil {
		return fmt.Errorf("%s: update batch stage: %w", op, err)
	}

	return requireAffected(res, op, models.ErrBatchNotFound)
}

func (r *Repository) MarkBatchRetry(ctx context.Context, runID, batchID string, stage models.Stage, reason string) error {
	const op = "Repository.MarkBatchRetry"

	return r.setBatchStageStatus(ctx, op, runID, batchID, stage, models.BatchStatusPending, reason)
}

func (r *Repository) MarkBatchDead(ctx context.Context, runID, batchID string, stage models.Stage, reason string) error {
	const op = "Repository.MarkBatchDead"

	return r.setBatchStageStatus(ctx, op, runID, batchID, stage, models.BatchStatusDead, reason)
}

func (r *Repository) setBatchStageStatus(
	ctx context.Context,
	op, runID, batchID string,
	stage models.Stage,
	status models.BatchStatus,
	reason string,
) error {
	res, err := r.q(ctx).ExecContext(ctx, `
		UPDATE batch_stages
		SET status = $4::batch_status,
			error = $5,
			lease_until = NULL
		WHERE run_id = $1::uuid
		  AND batch_id = $2::uuid
		  AND stage = $3::stage_name
	`, runID, batchID, string(stage), string(status), reason)
	if err != nil {
		return fmt.Errorf("%s: update batch stage: %w", op, err)
	}

	return requireAffected(res, op, models.ErrBatchNotFound)
}

func (r *Repository) ListStageProgress(ctx context.Context, runID string, stages ...models.Stage) ([]models.StageProgress, error) {
	if len(stages) == 0 {
		stages = models.BatchStages()
	}

	rows, err := queryList(ctx, r, "Repository.ListStageProgress", scanStageProgress, `
		SELECT
			stage::text,
			count(*) FILTER (WHERE status = 'done') AS done,
			count(*) FILTER (WHERE status IN ('failed', 'dead')) AS failed,
			count(*) AS total
		FROM batch_stages
		WHERE run_id = $1::uuid
		  AND stage = ANY($2::stage_name[])
		GROUP BY stage
	`, runID, stageArray(stages))
	if err != nil {
		return nil, err
	}

	byStage := make(map[models.Stage]models.StageProgress, len(rows))
	for _, p := range rows {
		byStage[p.Stage] = p
	}

	progress := make([]models.StageProgress, 0, len(stages))
	for _, stage := range stages {
		p := byStage[stage]
		p.Stage = stage
		progress = append(progress, p)
	}

	return progress, nil
}

func (r *Repository) MarkEventProcessed(ctx context.Context, runID, batchID string, stage models.Stage, attempt int) (bool, error) {
	const op = "Repository.MarkEventProcessed"

	res, err := r.q(ctx).ExecContext(ctx, `
		INSERT INTO processed_events (run_id, batch_id, stage, attempt)
		VALUES ($1::uuid, $2::uuid, $3::stage_name, $4)
		ON CONFLICT DO NOTHING
	`, runID, batchID, string(stage), attempt)
	if err != nil {
		return false, fmt.Errorf("%s: insert processed event: %w", op, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%s: rows affected: %w", op, err)
	}

	return affected == 1, nil
}

func (r *Repository) ListExpiredBatchStages(ctx context.Context, limit int) ([]*models.BatchStage, error) {
	return queryList(ctx, r, "Repository.ListExpiredBatchStages", scanBatchStage, selectBatchStageSQL+`
		WHERE bs.status = 'dispatched'
		  AND bs.lease_until < now()
		ORDER BY bs.lease_until
		LIMIT $1
	`, limit)
}

func (r *Repository) ListBatchOutputs(ctx context.Context, runID string) ([]models.BatchOutputs, error) {
	return queryList(ctx, r, "Repository.ListBatchOutputs", scanBatchOutputs, `
		SELECT
			b.batch_id::text,
			b.deal_count,
			cf.output_links,
			pv.output_links
		FROM batches b
		LEFT JOIN batch_stages cf
			ON cf.run_id = b.run_id AND cf.batch_id = b.batch_id AND cf.stage = 'cashflow'
		LEFT JOIN batch_stages pv
			ON pv.run_id = b.run_id AND pv.batch_id = b.batch_id AND pv.stage = 'pv'
		WHERE b.run_id = $1::uuid
		ORDER BY b.seq
	`, runID)
}

func stageArray(stages []models.Stage) any {
	out := make([]string, 0, len(stages))
	for _, stage := range stages {
		out = append(out, string(stage))
	}

	return pq.Array(out)
}
