package config

import (
	"errors"
	"flag"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	HTTP     HTTP `yaml:"http"`
	Database struct {
		DSN string `yaml:"dsn" env:"DB_DSN" env-default:"postgres://postgres:postgres@localhost:5432/control_plane?sslmode=disable"`
	} `yaml:"database"`
	NATS struct {
		URL    string `yaml:"url" env:"NATS_URL" env-default:"nats://localhost:4222"`
		Stream string `yaml:"stream" env:"NATS_STREAM" env-default:"CONTROL_PLANE"`
		Prefix string `yaml:"prefix" env:"NATS_PREFIX" env-default:"calc"`
	} `yaml:"nats"`
	S3 struct {
		Endpoint        string `yaml:"endpoint" env:"S3_ENDPOINT" env-default:"localhost:9000"`
		Region          string `yaml:"region" env:"S3_REGION" env-default:"us-east-1"`
		AccessKey       string `yaml:"access_key" env:"S3_ACCESS_KEY" env-default:"minioadmin"`
		SecretKey       string `yaml:"secret_key" env:"S3_SECRET_KEY" env-default:"minioadmin"`
		UseSSL          bool   `yaml:"use_ssl" env:"S3_USE_SSL" env-default:"false"`
		CuratedBucket   string `yaml:"curated_bucket" env:"S3_CURATED_BUCKET" env-default:"curated"`
		ArtifactsBucket string `yaml:"artifacts_bucket" env:"S3_ARTIFACTS_BUCKET" env-default:"artifacts"`
		PreparedPrefix  string `yaml:"prepared_prefix" env:"S3_PREPARED_PREFIX" env-default:"source=core_banking/"`
		PreparedKey     string `yaml:"prepared_key" env:"S3_PREPARED_KEY"`
	} `yaml:"s3"`
	Run struct {
		DefaultPortfolioID string        `yaml:"default_portfolio_id" env:"RUN_DEFAULT_PORTFOLIO_ID" env-default:"{snapshot_id}"`
		DefaultReportDate  string        `yaml:"default_report_dttm" env:"RUN_DEFAULT_REPORT_DTTM" env-default:"1970-01-01"`
		DefaultCurvesURI   string        `yaml:"default_curves_uri" env:"RUN_DEFAULT_CURVES_URI" env-default:"s3://curated/curve_snapshots/default/curves.parquet"`
		BatchSize          int           `yaml:"batch_size" env:"RUN_BATCH_SIZE" env-default:"10000"`
		MaxBatches         int           `yaml:"max_batches" env:"RUN_MAX_BATCHES" env-default:"10000"`
		MaxAttempts        int           `yaml:"max_attempts" env:"RUN_MAX_ATTEMPTS" env-default:"3"`
		LeaseTTL           time.Duration `yaml:"lease_ttl" env:"RUN_LEASE_TTL" env-default:"10m"`
		ScanInterval       time.Duration `yaml:"scan_interval" env:"RUN_SCAN_INTERVAL" env-default:"2s"`
		PublishTimeout     time.Duration `yaml:"publish_timeout" env:"RUN_PUBLISH_TIMEOUT" env-default:"5s"`
	} `yaml:"run"`
}
type HTTP struct {
	Port            int           `yaml:"port" env:"HTTP_PORT" env-default:"8081"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" env:"HTTP_SHUTDOWN_TIMEOUT" env-default:"15s"`
}

func MustLoad() *Config {
	configPath := fetchConfigPath()
	if configPath == "" {
		panic("config path is empty")
	}
	if _, err := os.Stat(configPath); errors.Is(err, os.ErrNotExist) {
		panic("config file does not exist: " + configPath)
	}
	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		panic("failed to read config: " + err.Error())
	}
	return &cfg
}

func fetchConfigPath() string {
	var path string
	flag.StringVar(&path, "config", "", "path to config file")
	flag.Parse()
	if path == "" {
		path = os.Getenv("CONFIG_PATH")
	}
	return path
}
