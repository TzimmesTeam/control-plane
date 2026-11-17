package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/parquet-go/parquet-go"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

func parquetData[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[T](&buf, parquet.MaxRowsPerRowGroup(2))
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
func TestPlanProjectsIDsAndWritesRealParquetBatches(t *testing.T) {
	type prepared struct {
		ID       int64   `parquet:"deal_id"`
		Currency *int32  `parquet:"currency_id,optional"`
		Dates    []int64 `parquet:"interest_payment,list"`
	}
	data := parquetData(t, []prepared{{ID: 1, Dates: []int64{1, 2}}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5}})
	stored := map[string][]byte{}
	put := func(_ context.Context, uri string, body []byte, contentType string) error {
		stored[uri] = body
		return nil
	}
	run := &models.Run{ID: "run-1", PlanningToken: "plan-1"}
	batches, err := planReader(context.Background(), run, bytes.NewReader(data), int64(len(data)), 3, 10, "artifacts", put)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || batches[0].DealCount != 3 || batches[1].DealCount != 2 {
		t.Fatalf("incorrect batches: %+v", batches)
	}
	var ids []int64
	for _, batch := range batches {
		if !strings.Contains(batch.IDsURI, "/plans/plan-1/") {
			t.Fatal("plans can overwrite each other")
		}
		body := stored[batch.IDsURI]
		reader := parquet.NewGenericReader[idRow](bytes.NewReader(body))
		rows := make([]idRow, 10)
		n, err := reader.Read(rows)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		reader.Close()
		for _, row := range rows[:n] {
			ids = append(ids, row.ID)
		}
	}
	if fmt.Sprint(ids) != "[1 2 3 4 5]" {
		t.Fatalf("lost or reordered deals: %v", ids)
	}
}
func TestPlanRejectsInvalidParquetAndLimits(t *testing.T) {
	valid := parquetData(t, []idRow{{ID: 1}, {ID: 2}, {ID: 3}})
	missing := parquetData(t, []struct{ Other int64 }{{Other: 1}})
	wrongType := parquetData(t, []struct {
		ID string `parquet:"deal_id"`
	}{{ID: "1"}})
	null := parquetData(t, []dealRef{{}})
	noop := func(context.Context, string, []byte, string) error { return nil }
	for name, data := range map[string][]byte{"corrupt": []byte("bad"), "missing": missing, "wrong_type": wrongType, "null": null} {
		t.Run(name, func(t *testing.T) {
			if _, err := planReader(context.Background(), &models.Run{}, bytes.NewReader(data), int64(len(data)), 2, 10, "artifacts", noop); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
	if _, err := planReader(context.Background(), &models.Run{}, bytes.NewReader(valid), int64(len(valid)), 1, 2, "artifacts", noop); err == nil {
		t.Fatal("max_batches ignored")
	}
	fail := func(context.Context, string, []byte, string) error { return errors.New("S3 down") }
	if _, err := planReader(context.Background(), &models.Run{}, bytes.NewReader(valid), int64(len(valid)), 2, 10, "artifacts", fail); err == nil {
		t.Fatal("upload failure ignored")
	}
	empty := parquetData(t, []idRow{})
	batches, err := planReader(context.Background(), &models.Run{}, bytes.NewReader(empty), int64(len(empty)), 2, 10, "artifacts", noop)
	if err != nil || len(batches) != 0 {
		t.Fatalf("empty parquet: %v %v", batches, err)
	}
}
func TestResolveSnapshotAcrossDatesAndRejectAmbiguity(t *testing.T) {
	keys := []string{"source=x/ingest_date=2020-01-01/snapshot_id=ab12/deals.parquet", "source=x/ingest_date=2020-01-01/snapshot_id=ab123/deals.parquet"}
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body strings.Builder
		fmt.Fprint(&body, `<ListBucketResult><Name>curated</Name><IsTruncated>false</IsTruncated>`)
		for _, key := range keys {
			fmt.Fprintf(&body, "<Contents><Key>%s</Key><Size>10</Size></Contents>", key)
		}
		fmt.Fprint(&body, "</ListBucketResult>")
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/xml"}}, Body: io.NopCloser(strings.NewReader(body.String())), Request: r}, nil
	})
	client, err := New(Config{Endpoint: "localhost:9000", Region: "us-east-1", CuratedBucket: "curated", PreparedPrefix: "source=x/"})
	if err != nil {
		t.Fatal(err)
	}
	client.mc, err = minio.New("localhost:9000", &minio.Options{Region: "us-east-1", Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	uri, err := client.Resolve(context.Background(), "ab12")
	if err != nil || !strings.Contains(uri, "2020-01-01/snapshot_id=ab12/") {
		t.Fatalf("date guessed or wrong ID matched: %s %v", uri, err)
	}
	keys = append(keys, "source=x/ingest_date=2020-01-02/snapshot_id=ab12/deals.parquet")
	if _, err := client.Resolve(context.Background(), "ab12"); err == nil {
		t.Fatal("ambiguous snapshot accepted")
	}
	if _, err := client.Resolve(context.Background(), "missing"); err == nil {
		t.Fatal("missing snapshot accepted")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestPreparedKeyMustIdentifySnapshot(t *testing.T) {
	if _, err := New(Config{PreparedKey: "deals.parquet"}); err == nil {
		t.Fatal("constant snapshot key accepted")
	}
	c, err := New(Config{Endpoint: "localhost:9000", PreparedKey: "snapshots/{snapshot_id}/deals.parquet", CuratedBucket: "curated"})
	if err != nil {
		t.Fatal(err)
	}
	uri, err := c.Resolve(context.Background(), "ab12")
	if err != nil || uri != "s3://curated/snapshots/ab12/deals.parquet" {
		t.Fatal(uri, err)
	}
}
