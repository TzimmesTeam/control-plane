package service

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/storage/s3"
	"github.com/google/uuid"
)

const (
	csvSeparator    = ';'
	unknownCurrency = "UNKNOWN"
	maxReportedDups = 10
	utf8BOM         = "\xef\xbb\xbf"

	colDealID         = "deal_id"
	colCurrency       = "currency_id"
	colReportDttm     = "report_dttm"
	colMaturityDttm   = "maturity_dttm"
	colRegularPeriod  = "regular_payment_rule_period_cd"
	colInterestPeriod = "interest_payment_rule_period_cd"
)

var batchNamespace = uuid.MustParse("6b3f6a52-6c1e-4d0e-9a57-0c3c2f6f1d4e")

var (
	ErrEmptyInput          = errors.New("input file is empty")
	ErrNoDeals             = errors.New("input file has no deals")
	ErrMissingDealIDColumn = errors.New("input file has no deal_id column")
	ErrEmptyDealID         = errors.New("empty deal_id")
	ErrDuplicateDealID     = errors.New("duplicate deal_id")
	ErrDealCountMismatch   = errors.New("sum of batch deal counts does not match total deals")
	ErrTooManyBatches      = errors.New("too many batches")
)

type BatchSink interface {
	InsertBatch(ctx context.Context, batch *models.Batch) error
}

type BlobWriter interface {
	Put(ctx context.Context, bucket, key string, data []byte) error
}

type SplitterConfig struct {
	Bucket       string
	TargetWeight int64
	MaxBatches   int
}

type SplitResult struct {
	TotalDeals   int
	TotalBatches int
}

type Splitter struct {
	log  *slog.Logger
	sink BatchSink
	blob BlobWriter
	cfg  SplitterConfig
}

func NewSplitter(log *slog.Logger, sink BatchSink, blob BlobWriter, cfg SplitterConfig) (*Splitter, error) {
	const op = "service.NewSplitter"

	if log == nil {
		log = slog.Default()
	}
	if sink == nil || blob == nil {
		return nil, fmt.Errorf("%s: batch sink and blob writer are required", op)
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("%s: bucket is required", op)
	}
	if cfg.TargetWeight <= 0 {
		return nil, fmt.Errorf("%s: target weight must be positive", op)
	}

	return &Splitter{log: log, sink: sink, blob: blob, cfg: cfg}, nil
}

type columns struct {
	dealID         int
	currency       int
	reportDttm     int
	maturityDttm   int
	regularPeriod  int
	interestPeriod int
}

type batchBuffer struct {
	ids    []string
	weight int64
}

type splitState struct {
	runID        string
	cols         columns
	line         int
	seen         map[string]struct{}
	open         map[string]*batchBuffer
	seq          int
	totalDeals   int
	flushedDeals int
	duplicates   []string
}

func (sp *Splitter) Split(ctx context.Context, runID string, r io.Reader) (SplitResult, error) {
	const op = "Splitter.Split"

	reader := csv.NewReader(r)
	reader.Comma = csvSeparator
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true
	reader.LazyQuotes = true

	cols, err := readHeader(reader)
	if err != nil {
		return SplitResult{}, fmt.Errorf("%s: %w", op, err)
	}

	st := &splitState{
		runID: runID,
		cols:  cols,
		line:  1,
		seen:  make(map[string]struct{}),
		open:  make(map[string]*batchBuffer),
	}

	if err := sp.readRecords(ctx, reader, st); err != nil {
		return SplitResult{}, fmt.Errorf("%s: %w", op, err)
	}
	if err := sp.flushAll(ctx, st); err != nil {
		return SplitResult{}, fmt.Errorf("%s: %w", op, err)
	}
	if err := verifySplit(st); err != nil {
		return SplitResult{}, fmt.Errorf("%s: %w", op, err)
	}

	return SplitResult{TotalDeals: st.totalDeals, TotalBatches: st.seq}, nil
}

func readHeader(reader *csv.Reader) (columns, error) {
	header, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return columns{}, ErrEmptyInput
	}
	if err != nil {
		return columns{}, fmt.Errorf("read header: %w", err)
	}

	index := make(map[string]int, len(header))
	for i, name := range header {
		name = strings.TrimSpace(strings.TrimPrefix(name, utf8BOM))
		index[strings.ToLower(name)] = i
	}

	lookup := func(name string) int {
		if i, ok := index[name]; ok {
			return i
		}
		return -1
	}

	cols := columns{
		dealID:         lookup(colDealID),
		currency:       lookup(colCurrency),
		reportDttm:     lookup(colReportDttm),
		maturityDttm:   lookup(colMaturityDttm),
		regularPeriod:  lookup(colRegularPeriod),
		interestPeriod: lookup(colInterestPeriod),
	}
	if cols.dealID < 0 {
		return columns{}, ErrMissingDealIDColumn
	}

	return cols, nil
}

func (sp *Splitter) readRecords(ctx context.Context, reader *csv.Reader, st *splitState) error {
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read line %d: %w", st.line+1, err)
		}
		st.line++

		if isBlankRecord(record) {
			continue
		}
		if err := sp.processRecord(ctx, st, record); err != nil {
			return err
		}
		if len(st.duplicates) >= maxReportedDups {
			return nil
		}
	}
}

func (sp *Splitter) processRecord(ctx context.Context, st *splitState, record []string) error {
	dealID := field(record, st.cols.dealID)
	if dealID == "" {
		return fmt.Errorf("%w at line %d", ErrEmptyDealID, st.line)
	}
	if _, dup := st.seen[dealID]; dup {
		st.duplicates = append(st.duplicates, strings.Clone(dealID))
		return nil
	}
	dealID = strings.Clone(dealID)
	st.seen[dealID] = struct{}{}

	currency := field(record, st.cols.currency)
	if currency == "" {
		currency = unknownCurrency
	}

	weight := estimateWeight(
		field(record, st.cols.regularPeriod),
		field(record, st.cols.interestPeriod),
		field(record, st.cols.reportDttm),
		field(record, st.cols.maturityDttm),
	)

	buf, ok := st.open[currency]
	if !ok {
		currency = strings.Clone(currency)
		buf = &batchBuffer{}
		st.open[currency] = buf
	}
	buf.ids = append(buf.ids, dealID)
	buf.weight += weight
	st.totalDeals++

	if buf.weight >= sp.cfg.TargetWeight && len(st.duplicates) == 0 {
		return sp.flush(ctx, st, currency, buf)
	}

	return nil
}

func (sp *Splitter) flushAll(ctx context.Context, st *splitState) error {
	if len(st.duplicates) > 0 {
		return nil
	}

	currencies := make([]string, 0, len(st.open))
	for currency, buf := range st.open {
		if len(buf.ids) > 0 {
			currencies = append(currencies, currency)
		}
	}
	slices.Sort(currencies)

	for _, currency := range currencies {
		if err := sp.flush(ctx, st, currency, st.open[currency]); err != nil {
			return err
		}
	}

	return nil
}

func (sp *Splitter) flush(ctx context.Context, st *splitState, currency string, buf *batchBuffer) error {
	st.seq++
	if sp.cfg.MaxBatches > 0 && st.seq > sp.cfg.MaxBatches {
		return fmt.Errorf("%w: limit is %d", ErrTooManyBatches, sp.cfg.MaxBatches)
	}

	batchID := batchIDFor(st.runID, st.seq)
	key := s3.IdsKey(st.runID, batchID)

	if err := sp.blob.Put(ctx, sp.cfg.Bucket, key, idsFile(buf.ids)); err != nil {
		return fmt.Errorf("write ids of batch %s: %w", batchID, err)
	}

	batch := &models.Batch{
		RunID:     st.runID,
		BatchID:   batchID,
		Seq:       st.seq,
		IdsURI:    s3.URI(sp.cfg.Bucket, key),
		DealCount: len(buf.ids),
		Weight:    buf.weight,
		Currency:  currency,
	}
	if err := sp.sink.InsertBatch(ctx, batch); err != nil {
		return fmt.Errorf("insert batch %s: %w", batchID, err)
	}

	sp.log.Debug("batch flushed",
		slog.String("run_id", st.runID),
		slog.String("batch_id", batchID),
		slog.Int("seq", st.seq),
		slog.String("currency", currency),
		slog.Int("deals", batch.DealCount),
		slog.Int64("weight", batch.Weight),
	)

	st.flushedDeals += len(buf.ids)
	buf.ids = buf.ids[:0]
	buf.weight = 0

	return nil
}

func verifySplit(st *splitState) error {
	if len(st.duplicates) > 0 {
		return fmt.Errorf("%w: %s", ErrDuplicateDealID, strings.Join(st.duplicates, ", "))
	}
	if st.totalDeals == 0 {
		return ErrNoDeals
	}
	if st.flushedDeals != st.totalDeals {
		return fmt.Errorf("%w: batches have %d, input has %d", ErrDealCountMismatch, st.flushedDeals, st.totalDeals)
	}

	return nil
}

func batchIDFor(runID string, seq int) string {
	return uuid.NewSHA1(batchNamespace, []byte(runID+":"+strconv.Itoa(seq))).String()
}

func idsFile(ids []string) []byte {
	size := 0
	for _, id := range ids {
		size += len(id) + 1
	}

	var buf bytes.Buffer
	buf.Grow(size)
	for _, id := range ids {
		buf.WriteString(id)
		buf.WriteByte('\n')
	}

	return buf.Bytes()
}

func field(record []string, idx int) string {
	if idx < 0 || idx >= len(record) {
		return ""
	}

	return strings.TrimSpace(record[idx])
}

func isBlankRecord(record []string) bool {
	for _, v := range record {
		if strings.TrimSpace(v) != "" {
			return false
		}
	}

	return true
}
