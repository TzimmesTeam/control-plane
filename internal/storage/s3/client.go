package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	retryAttempts  = 3
	retryBaseDelay = 200 * time.Millisecond

	errCodeNoSuchKey = "NoSuchKey"
	contentTypeJSON  = "application/json"
	contentTypeText  = "text/plain; charset=utf-8"
)

type Config struct {
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Buckets   []string
}

type Client struct {
	mc  *minio.Client
	log *slog.Logger
}

func New(ctx context.Context, cfg Config, log *slog.Logger) (*Client, error) {
	const op = "s3.New"

	if log == nil {
		log = slog.Default()
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("%s: endpoint is required", op)
	}

	mc, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: create minio client: %w", op, err)
	}

	c := &Client{mc: mc, log: log}

	for _, bucket := range cfg.Buckets {
		if err := c.checkBucket(ctx, bucket); err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
	}

	return c, nil
}

func (c *Client) Put(ctx context.Context, bucket, key string, data []byte) error {
	const op = "s3.Put"

	if c == nil || c.mc == nil {
		return fmt.Errorf("%s: client is not initialized", op)
	}

	opts := minio.PutObjectOptions{ContentType: contentType(key)}

	err := c.withRetry(ctx, op, func() error {
		_, err := c.mc.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), opts)
		return err
	})
	if err != nil {
		return fmt.Errorf("%s: put %s/%s: %w", op, bucket, key, err)
	}

	return nil
}

func (c *Client) Open(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	const op = "s3.Open"

	if c == nil || c.mc == nil {
		return nil, fmt.Errorf("%s: client is not initialized", op)
	}

	var obj *minio.Object
	err := c.withRetry(ctx, op, func() error {
		o, err := c.mc.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
		if err != nil {
			return err
		}
		if _, err := o.Stat(); err != nil {
			if closeErr := o.Close(); closeErr != nil {
				c.log.Warn("s3 object close failed", slog.Any("error", closeErr))
			}
			return err
		}
		obj = o
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%s: open %s/%s: %w", op, bucket, key, err)
	}

	return obj, nil
}

func (c *Client) Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) error {
	const op = "s3.Copy"

	if c == nil || c.mc == nil {
		return fmt.Errorf("%s: client is not initialized", op)
	}

	dst := minio.CopyDestOptions{Bucket: dstBucket, Object: dstKey}
	src := minio.CopySrcOptions{Bucket: srcBucket, Object: srcKey}

	err := c.withRetry(ctx, op, func() error {
		_, err := c.mc.CopyObject(ctx, dst, src)
		return err
	})
	if err != nil {
		return fmt.Errorf("%s: copy %s/%s to %s/%s: %w", op, srcBucket, srcKey, dstBucket, dstKey, err)
	}

	return nil
}

func (c *Client) CopyPrefix(ctx context.Context, srcBucket, srcPrefix, dstBucket, dstPrefix string) (int, error) {
	const op = "s3.CopyPrefix"

	if c == nil || c.mc == nil {
		return 0, fmt.Errorf("%s: client is not initialized", op)
	}

	keys, err := c.listKeys(ctx, srcBucket, srcPrefix)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	for _, key := range keys {
		dstKey := dstPrefix + strings.TrimPrefix(key, srcPrefix)
		if err := c.Copy(ctx, srcBucket, key, dstBucket, dstKey); err != nil {
			return 0, fmt.Errorf("%s: %w", op, err)
		}
	}

	return len(keys), nil
}

func (c *Client) Exists(ctx context.Context, bucket, key string) (bool, error) {
	const op = "s3.Exists"

	if c == nil || c.mc == nil {
		return false, fmt.Errorf("%s: client is not initialized", op)
	}

	exists := false
	err := c.withRetry(ctx, op, func() error {
		_, err := c.mc.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
		if err == nil {
			exists = true
			return nil
		}
		if minio.ToErrorResponse(err).Code == errCodeNoSuchKey {
			exists = false
			return nil
		}
		return err
	})
	if err != nil {
		return false, fmt.Errorf("%s: stat %s/%s: %w", op, bucket, key, err)
	}

	return exists, nil
}

func (c *Client) listKeys(ctx context.Context, bucket, prefix string) ([]string, error) {
	var keys []string

	err := c.withRetry(ctx, "s3.listKeys", func() error {
		keys = keys[:0]
		for obj := range c.mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if obj.Err != nil {
				return obj.Err
			}
			if strings.HasSuffix(obj.Key, "/") {
				continue
			}
			keys = append(keys, obj.Key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list %s/%s: %w", bucket, prefix, err)
	}

	return keys, nil
}

func (c *Client) checkBucket(ctx context.Context, bucket string) error {
	var exists bool

	err := c.withRetry(ctx, "s3.checkBucket", func() error {
		var err error
		exists, err = c.mc.BucketExists(ctx, bucket)
		return err
	})
	if err != nil {
		return fmt.Errorf("check bucket %s: %w", bucket, err)
	}
	if !exists {
		return fmt.Errorf("bucket %s does not exist", bucket)
	}

	return nil
}

func (c *Client) withRetry(ctx context.Context, op string, fn func() error) error {
	delay := retryBaseDelay

	var err error
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt == retryAttempts {
			break
		}

		c.log.Warn("s3 operation failed, retrying",
			slog.String("op", op),
			slog.Int("attempt", attempt),
			slog.Any("error", err),
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}

	return err
}

func contentType(key string) string {
	if strings.HasSuffix(key, ".json") {
		return contentTypeJSON
	}

	return contentTypeText
}
