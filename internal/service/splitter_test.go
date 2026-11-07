package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testRunID       = "11111111-1111-4111-8111-111111111111"
	testBatchID     = "22222222-2222-4222-8222-222222222222"
	testPortfolioID = "99999999-9999-4999-8999-999999999999"
	testLake        = "data-lake"

	csvHeader = "deal_id;currency_id;report_dttm;maturity_dttm;regular_payment_rule_period_cd;interest_payment_rule_period_cd\n"
)

type batchSinkMock struct{ mock.Mock }

func (m *batchSinkMock) InsertBatch(ctx context.Context, batch *models.Batch) error {
	return m.Called(ctx, batch).Error(0)
}

type blobWriterMock struct{ mock.Mock }

func (m *blobWriterMock) Put(ctx context.Context, bucket, key string, data []byte) error {
	return m.Called(ctx, bucket, key, data).Error(0)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestSplitter(t *testing.T, sink *batchSinkMock, blob *blobWriterMock, targetWeight int64) *Splitter {
	t.Helper()

	sp, err := NewSplitter(testLogger(), sink, blob, SplitterConfig{
		Bucket:       testLake,
		TargetWeight: targetWeight,
		MaxBatches:   100,
	})
	require.NoError(t, err)

	return sp
}

func recordBatches(sink *batchSinkMock, blob *blobWriterMock) (*[]*models.Batch, map[string]string) {
	batches := make([]*models.Batch, 0)
	files := make(map[string]string)

	sink.On("InsertBatch", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		if b, ok := args.Get(1).(*models.Batch); ok {
			batches = append(batches, b)
		}
	}).Return(nil)
	blob.On("Put", mock.Anything, testLake, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		key := ret[string](args, 2)
		data := ret[[]byte](args, 3)
		files[key] = string(data)
	}).Return(nil)

	return &batches, files
}

func TestSplitOneBatch(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	batches, files := recordBatches(sink, blob)
	sp := newTestSplitter(t, sink, blob, 1000)

	input := csvHeader +
		"1;RUB;31.08.2022;31.08.2023;1M;\n" +
		"3;RUB;31.08.2022;31.08.2023;ENDDATE;\n" +
		"7;RUB;31.08.2022;31.08.2023;;3M\n"

	result, err := sp.Split(context.Background(), testRunID, strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, SplitResult{TotalDeals: 3, TotalBatches: 1}, result)

	require.Len(t, *batches, 1)
	b := (*batches)[0]
	require.Equal(t, 1, b.Seq)
	require.Equal(t, 3, b.DealCount)
	require.Equal(t, "RUB", b.Currency)
	require.Equal(t, int64(12+1+4), b.Weight)
	require.Equal(t, batchIDFor(testRunID, 1), b.BatchID)
	require.Equal(t, "s3://data-lake/runs/"+testRunID+"/batches/"+b.BatchID+".ids", b.IdsURI)
	require.Equal(t, "1\n3\n7\n", files["runs/"+testRunID+"/batches/"+b.BatchID+".ids"])
}

func TestSplitSeveralBatchesByWeight(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	batches, _ := recordBatches(sink, blob)
	sp := newTestSplitter(t, sink, blob, 2)

	input := csvHeader +
		"1;RUB;;;ENDDATE;\n" +
		"2;RUB;;;ENDDATE;\n" +
		"3;RUB;;;ENDDATE;\n" +
		"4;RUB;;;ENDDATE;\n" +
		"5;RUB;;;ENDDATE;\n"

	result, err := sp.Split(context.Background(), testRunID, strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, SplitResult{TotalDeals: 5, TotalBatches: 3}, result)

	counts := make([]int, 0, len(*batches))
	for _, b := range *batches {
		counts = append(counts, b.DealCount)
	}
	require.Equal(t, []int{2, 2, 1}, counts)
}

func TestSplitByCurrency(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	batches, files := recordBatches(sink, blob)
	sp := newTestSplitter(t, sink, blob, 1000)

	input := csvHeader +
		"1;USD;;;ENDDATE;\n" +
		"2;RUB;;;ENDDATE;\n" +
		"3;USD;;;ENDDATE;\n" +
		"4;;;;ENDDATE;\n"

	result, err := sp.Split(context.Background(), testRunID, strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, SplitResult{TotalDeals: 4, TotalBatches: 3}, result)

	currencies := make([]string, 0, len(*batches))
	for _, b := range *batches {
		currencies = append(currencies, b.Currency)
	}
	require.Equal(t, []string{"RUB", unknownCurrency, "USD"}, currencies)

	usd := (*batches)[2]
	require.Equal(t, "1\n3\n", files["runs/"+testRunID+"/batches/"+usd.BatchID+".ids"])
}

func TestSplitDuplicateDealID(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	sp := newTestSplitter(t, sink, blob, 1000)

	input := csvHeader +
		"1;RUB;;;ENDDATE;\n" +
		"2;RUB;;;ENDDATE;\n" +
		"1;RUB;;;ENDDATE;\n"

	_, err := sp.Split(context.Background(), testRunID, strings.NewReader(input))
	require.ErrorIs(t, err, ErrDuplicateDealID)
	require.Contains(t, err.Error(), ": 1")
	sink.AssertNotCalled(t, "InsertBatch", mock.Anything, mock.Anything)
	blob.AssertNotCalled(t, "Put", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestSplitReportsAtMostTenDuplicates(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	sp := newTestSplitter(t, sink, blob, 1000)

	var sb strings.Builder
	sb.WriteString(csvHeader)
	sb.WriteString("1;RUB;;;ENDDATE;\n")
	for range 20 {
		sb.WriteString("1;RUB;;;ENDDATE;\n")
	}

	_, err := sp.Split(context.Background(), testRunID, strings.NewReader(sb.String()))
	require.ErrorIs(t, err, ErrDuplicateDealID)
	require.Equal(t, maxReportedDups, strings.Count(err.Error(), "1, ")+1)
}

func TestSplitEmptyDealID(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	sp := newTestSplitter(t, sink, blob, 1000)

	input := csvHeader + "1;RUB;;;ENDDATE;\n" + ";RUB;;;ENDDATE;\n"

	_, err := sp.Split(context.Background(), testRunID, strings.NewReader(input))
	require.ErrorIs(t, err, ErrEmptyDealID)
	require.Contains(t, err.Error(), "line 3")
}

func TestSplitEmptyFile(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	sp := newTestSplitter(t, sink, blob, 1000)

	_, err := sp.Split(context.Background(), testRunID, strings.NewReader(""))
	require.ErrorIs(t, err, ErrEmptyInput)
}

func TestSplitHeaderOnly(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	sp := newTestSplitter(t, sink, blob, 1000)

	_, err := sp.Split(context.Background(), testRunID, strings.NewReader(csvHeader))
	require.ErrorIs(t, err, ErrNoDeals)
}

func TestSplitMissingDealIDColumn(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	sp := newTestSplitter(t, sink, blob, 1000)

	input := "id;currency_id\n1;RUB\n"

	_, err := sp.Split(context.Background(), testRunID, strings.NewReader(input))
	require.ErrorIs(t, err, ErrMissingDealIDColumn)
}

func TestSplitColumnsFoundByName(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	batches, _ := recordBatches(sink, blob)
	sp := newTestSplitter(t, sink, blob, 1000)

	input := "\xef\xbb\xbfcurrency_id; Deal_ID ;other\nEUR;42;x\n"

	result, err := sp.Split(context.Background(), testRunID, strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, 1, result.TotalDeals)
	require.Equal(t, "EUR", (*batches)[0].Currency)
}

func TestSplitGarbageRowDoesNotBreakPass(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	batches, _ := recordBatches(sink, blob)
	sp := newTestSplitter(t, sink, blob, 1000)

	input := csvHeader +
		"1;RUB;31.08.2022;31.08.2023;1M;\n" +
		"2;RUB;not a date;???;weird\"quote;\n" +
		"3\n" +
		";;;;;\n" +
		"4;RUB;31.08.2022;31.08.2023;1M;;extra;columns\n"

	result, err := sp.Split(context.Background(), testRunID, strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, SplitResult{TotalDeals: 4, TotalBatches: 2}, result)

	total := 0
	for _, b := range *batches {
		total += b.DealCount
	}
	require.Equal(t, 4, total)
}

func TestSplitStorageError(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	blob.On("Put", mock.Anything, testLake, mock.Anything, mock.Anything).Return(errors.New("s3 down"))
	sp := newTestSplitter(t, sink, blob, 1000)

	_, err := sp.Split(context.Background(), testRunID, strings.NewReader(csvHeader+"1;RUB;;;ENDDATE;\n"))
	require.ErrorContains(t, err, "s3 down")
	sink.AssertNotCalled(t, "InsertBatch", mock.Anything, mock.Anything)
}

func TestSplitTooManyBatches(t *testing.T) {
	sink, blob := &batchSinkMock{}, &blobWriterMock{}
	recordBatches(sink, blob)

	sp, err := NewSplitter(testLogger(), sink, blob, SplitterConfig{Bucket: testLake, TargetWeight: 1, MaxBatches: 2})
	require.NoError(t, err)

	input := csvHeader + "1;RUB;;;ENDDATE;\n" + "2;RUB;;;ENDDATE;\n" + "3;RUB;;;ENDDATE;\n"

	_, err = sp.Split(context.Background(), testRunID, strings.NewReader(input))
	require.ErrorIs(t, err, ErrTooManyBatches)
}

func TestBatchIDIsDeterministic(t *testing.T) {
	require.Equal(t, batchIDFor(testRunID, 1), batchIDFor(testRunID, 1))
	require.NotEqual(t, batchIDFor(testRunID, 1), batchIDFor(testRunID, 2))
	require.NotEqual(t, batchIDFor(testRunID, 1), batchIDFor(testBatchID, 1))
}
