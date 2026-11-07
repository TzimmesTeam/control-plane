package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

type Config struct {
	DSN          string
	MaxOpenConns int
	MaxIdleConns int
}

type Repository struct {
	db  *sql.DB
	log *slog.Logger
}

type txKey struct{}

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type rowScanner interface {
	Scan(dest ...any) error
}

func New(ctx context.Context, cfg Config, log *slog.Logger) (*Repository, error) {
	const op = "postgres.New"

	if log == nil {
		log = slog.Default()
	}

	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("%s: open db: %w", op, err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)

	if err := db.PingContext(ctx); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, fmt.Errorf("%s: ping db: %w; close db: %v", op, err, closeErr)
		}
		return nil, fmt.Errorf("%s: ping db: %w", op, err)
	}

	return &Repository{db: db, log: log}, nil
}

func (r *Repository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}

	if err := r.db.Close(); err != nil {
		return fmt.Errorf("postgres.Close: %w", err)
	}

	return nil
}

func (r *Repository) Ping(ctx context.Context) error {
	if r == nil || r.db == nil {
		return errors.New("postgres.Ping: repository is not initialized")
	}

	if err := r.db.PingContext(ctx); err != nil {
		return fmt.Errorf("postgres.Ping: %w", err)
	}

	return nil
}

func (r *Repository) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	const op = "Repository.WithinTx"

	if _, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return fn(ctx)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: begin tx: %w", op, err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			r.log.Error("tx rollback failed", slog.Any("error", rbErr))
		}
	}()

	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: commit: %w", op, err)
	}

	return nil
}

func (r *Repository) q(ctx context.Context) querier {
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return tx
	}

	return r.db
}

func (r *Repository) closeRows(rows *sql.Rows, op string) {
	if err := rows.Close(); err != nil {
		r.log.Error("rows close failed", slog.String("op", op), slog.Any("error", err))
	}
}

func queryList[T any](
	ctx context.Context,
	r *Repository,
	op string,
	scan func(rowScanner) (T, error),
	query string,
	args ...any,
) ([]T, error) {
	rows, err := r.q(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: query: %w", op, err)
	}
	defer r.closeRows(rows, op)

	items := make([]T, 0)
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", op, err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: iterate: %w", op, err)
	}

	return items, nil
}

// ---------------------------------------------------------------- scanners

func scanRun(row rowScanner) (*models.Run, error) {
	run := &models.Run{}
	var status, stage string
	var finishedAt sql.NullTime

	err := row.Scan(
		&run.RunID,
		&run.PortfolioID,
		&run.ReportDttm,
		&run.SourceURI,
		&run.CurvesURI,
		&run.ContextURI,
		&status,
		&stage,
		&run.TotalDeals,
		&run.TotalBatches,
		&run.ReportLink,
		&run.Error,
		&run.CreatedAt,
		&run.UpdatedAt,
		&finishedAt,
	)
	if err != nil {
		return nil, err
	}

	run.Status = models.RunStatus(status)
	run.CurrentStage = models.Stage(stage)
	run.FinishedAt = timePtr(finishedAt)

	return run, nil
}

func scanBatch(row rowScanner) (*models.Batch, error) {
	b := &models.Batch{}

	err := row.Scan(&b.RunID, &b.BatchID, &b.Seq, &b.IdsURI, &b.DealCount, &b.Weight, &b.Currency, &b.CreatedAt)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func scanBatchStage(row rowScanner) (*models.BatchStage, error) {
	bs := &models.BatchStage{}
	var stage, status string
	var links, stats []byte
	var dispatchedAt, doneAt, leaseUntil sql.NullTime

	err := row.Scan(
		&bs.RunID,
		&bs.BatchID,
		&stage,
		&status,
		&bs.Attempt,
		&links,
		&stats,
		&bs.Error,
		&dispatchedAt,
		&doneAt,
		&leaseUntil,
		&bs.DealCount,
	)
	if err != nil {
		return nil, err
	}

	if err := unmarshalJSON(links, &bs.OutputLinks); err != nil {
		return nil, fmt.Errorf("decode output links: %w", err)
	}
	if err := unmarshalJSON(stats, &bs.Stats); err != nil {
		return nil, fmt.Errorf("decode stats: %w", err)
	}

	bs.Stage = models.Stage(stage)
	bs.Status = models.BatchStatus(status)
	bs.DispatchedAt = timePtr(dispatchedAt)
	bs.DoneAt = timePtr(doneAt)
	bs.LeaseUntil = timePtr(leaseUntil)

	return bs, nil
}

func scanStageProgress(row rowScanner) (models.StageProgress, error) {
	var p models.StageProgress
	var stage string

	if err := row.Scan(&stage, &p.Done, &p.Failed, &p.Total); err != nil {
		return models.StageProgress{}, err
	}
	p.Stage = models.Stage(stage)

	return p, nil
}

func scanBatchOutputs(row rowScanner) (models.BatchOutputs, error) {
	var o models.BatchOutputs
	var cashflow, pv []byte

	if err := row.Scan(&o.BatchID, &o.DealCount, &cashflow, &pv); err != nil {
		return models.BatchOutputs{}, err
	}
	if err := unmarshalJSON(cashflow, &o.Cashflow); err != nil {
		return models.BatchOutputs{}, fmt.Errorf("decode cashflow links: %w", err)
	}
	if err := unmarshalJSON(pv, &o.PV); err != nil {
		return models.BatchOutputs{}, fmt.Errorf("decode pv links: %w", err)
	}

	return o, nil
}

func scanAggregation(row rowScanner) (*models.Aggregation, error) {
	agg := &models.Aggregation{}
	var status string
	var stats []byte
	var dispatchedAt, doneAt, leaseUntil sql.NullTime

	err := row.Scan(
		&agg.RunID,
		&agg.PortfolioID,
		&status,
		&agg.Attempt,
		&agg.BatchesURI,
		&agg.ReportLink,
		&stats,
		&agg.Error,
		&dispatchedAt,
		&doneAt,
		&leaseUntil,
	)
	if err != nil {
		return nil, err
	}

	if err := unmarshalJSON(stats, &agg.Stats); err != nil {
		return nil, fmt.Errorf("decode stats: %w", err)
	}

	agg.Status = models.BatchStatus(status)
	agg.DispatchedAt = timePtr(dispatchedAt)
	agg.DoneAt = timePtr(doneAt)
	agg.LeaseUntil = timePtr(leaseUntil)

	return agg, nil
}

func scanString(row rowScanner) (string, error) {
	var s string
	err := row.Scan(&s)
	return s, err
}

// ---------------------------------------------------------------- helpers

func marshalOutputs(links []models.OutputLink, stats map[string]int64) (linksJSON, statsJSON any, err error) {
	if links != nil {
		raw, err := json.Marshal(links)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal output links: %w", err)
		}
		linksJSON = string(raw)
	}
	if stats != nil {
		raw, err := json.Marshal(stats)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal stats: %w", err)
		}
		statsJSON = string(raw)
	}

	return linksJSON, statsJSON, nil
}

func unmarshalJSON(raw []byte, dst any) error {
	if len(raw) == 0 {
		return nil
	}

	return json.Unmarshal(raw, dst)
}

func requireAffected(res sql.Result, op string, notFound error) error {
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: rows affected: %w", op, err)
	}
	if affected == 0 {
		return fmt.Errorf("%s: %w", op, notFound)
	}

	return nil
}

func timePtr(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}

	v := t.Time
	return &v
}
