package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/parquet-go/parquet-go"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

type Config struct {
	Endpoint        string
	Region          string
	AccessKey       string
	SecretKey       string
	UseSSL          bool
	CuratedBucket   string
	ArtifactsBucket string
	PreparedPrefix  string
	PreparedKey     string
}
type Client struct {
	mc  *minio.Client
	cfg Config
}

func New(cfg Config) (*Client, error) {
	if cfg.PreparedKey != "" && !strings.Contains(cfg.PreparedKey, "{snapshot_id}") {
		return nil, fmt.Errorf("prepared_key must contain {snapshot_id}")
	}
	mc, err := minio.New(cfg.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""), Secure: cfg.UseSSL, Region: cfg.Region})
	if err != nil {
		return nil, err
	}
	return &Client{mc: mc, cfg: cfg}, nil
}
func URI(bucket, key string) string { return "s3://" + bucket + "/" + key }
func ParseURI(uri string) (string, string, error) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	bucket, key, found := strings.Cut(rest, "/")
	if !ok || !found || bucket == "" || key == "" || strings.ContainsAny(bucket, "?# ") {
		return "", "", fmt.Errorf("invalid S3 object URI")
	}
	return bucket, key, nil
}

func (c *Client) Resolve(ctx context.Context, snapshot string) (string, error) {
	if c.cfg.PreparedKey != "" {
		return URI(c.cfg.CuratedBucket, strings.ReplaceAll(c.cfg.PreparedKey, "{snapshot_id}", snapshot)), nil
	}
	var key string
	suffix := "snapshot_id=" + snapshot + "/deals.parquet"
	for obj := range c.mc.ListObjects(ctx, c.cfg.CuratedBucket, minio.ListObjectsOptions{Prefix: c.cfg.PreparedPrefix, Recursive: true}) {
		if obj.Err != nil {
			return "", obj.Err
		}
		if obj.Key != suffix && !strings.HasSuffix(obj.Key, "/"+suffix) {
			continue
		}
		if key != "" {
			return "", fmt.Errorf("snapshot %s has multiple prepared objects; register prepared_uri explicitly", snapshot)
		}
		key = obj.Key
	}
	if key == "" {
		return "", fmt.Errorf("prepared object for snapshot %s not found", snapshot)
	}
	return URI(c.cfg.CuratedBucket, key), nil
}
func (c *Client) PutJSON(ctx context.Context, uri string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.put(ctx, uri, data, "application/json")
}
func (c *Client) put(ctx context.Context, uri string, data []byte, contentType string) error {
	bucket, key, err := ParseURI(uri)
	if err != nil {
		return err
	}
	_, err = c.mc.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: contentType})
	return err
}

type dealRef struct {
	ID *int64 `parquet:"deal_id,optional"`
}
type idRow struct {
	ID int64 `parquet:"deal_id"`
}

func (c *Client) Plan(ctx context.Context, run *models.Run, batchSize, maxBatches int) (batches []models.Batch, version, etag string, err error) {
	bucket, key, err := ParseURI(run.PreparedURI)
	if err != nil {
		return nil, "", "", err
	}
	info, err := c.mc.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, "", "", err
	}
	options := minio.GetObjectOptions{VersionID: info.VersionID}
	if err := options.SetMatchETag(info.ETag); err != nil {
		return nil, "", "", err
	}
	object, err := c.mc.GetObject(ctx, bucket, key, options)
	if err != nil {
		return nil, "", "", err
	}
	defer object.Close()
	batches, err = planReader(ctx, run, object, info.Size, batchSize, maxBatches, c.cfg.ArtifactsBucket, c.put)
	return batches, info.VersionID, info.ETag, err
}
func planReader(ctx context.Context, run *models.Run, input io.ReaderAt, size int64, batchSize, maxBatches int, artifactsBucket string, put func(context.Context, string, []byte, string) error) (batches []models.Batch, err error) {
	defer func() {
		if value := recover(); value != nil {
			batches = nil
			err = fmt.Errorf("invalid prepared Parquet: %v", value)
		}
	}()
	file, err := parquet.OpenFile(input, size)
	if err != nil {
		return nil, err
	}
	leaf, ok := file.Schema().Lookup("deal_id")
	if !ok || leaf.Node.Type().Kind() != parquet.Int64 {
		return nil, fmt.Errorf("prepared Parquet must have an int64 deal_id column")
	}
	reader := parquet.NewGenericReader[dealRef](file)
	defer reader.Close()
	batches = make([]models.Batch, 0)
	rows := make([]dealRef, batchSize)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, readErr := ioReadBatch(reader, rows)
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		if n > 0 {
			if len(batches) >= maxBatches {
				return nil, fmt.Errorf("snapshot exceeds max_batches=%d", maxBatches)
			}
			seq := len(batches) + 1
			id := uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("%s/batch/%d", run.ID, seq))).String()
			uri := URI(artifactsBucket, fmt.Sprintf("runs/%s/plans/%s/batches/seq=%d/ids.parquet", run.ID, run.PlanningToken, seq))
			data, err := encodeIDs(rows[:n])
			if err != nil {
				return nil, err
			}
			if err := put(ctx, uri, data, "application/vnd.apache.parquet"); err != nil {
				return nil, err
			}
			batches = append(batches, models.Batch{ID: id, RunID: run.ID, Seq: seq, DealCount: n, IDsURI: uri})
		}
		if readErr == io.EOF {
			return batches, nil
		}
	}
}

func ioReadBatch(reader *parquet.GenericReader[dealRef], rows []dealRef) (int, error) {
	count := 0
	for count < len(rows) {
		n, err := reader.Read(rows[count:])
		count += n
		if err != nil {
			return count, err
		}
		if n == 0 {
			return count, io.ErrNoProgress
		}
	}
	return count, nil
}
func encodeIDs(refs []dealRef) ([]byte, error) {
	rows := make([]idRow, len(refs))
	for i, ref := range refs {
		if ref.ID == nil {
			return nil, fmt.Errorf("prepared Parquet contains a null deal_id")
		}
		rows[i].ID = *ref.ID
	}
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[idRow](&buf)
	if _, err := writer.Write(rows); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
