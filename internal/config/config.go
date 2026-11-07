package config

import (
	"errors"
	"flag"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env      string   `yaml:"env" env:"ENV" env-default:"local"`
	HTTP     HTTP     `yaml:"http"`
	Database Database `yaml:"database"`
	NATS     NATS     `yaml:"nats"`
	S3       S3       `yaml:"s3"`
	Run      Run      `yaml:"run"`
}

type HTTP struct {
	Port            int           `yaml:"port" env:"HTTP_PORT" env-default:"8089"`
	ReadTimeout     time.Duration `yaml:"read_timeout" env:"HTTP_READ_TIMEOUT" env-default:"10s"`
	WriteTimeout    time.Duration `yaml:"write_timeout" env:"HTTP_WRITE_TIMEOUT" env-default:"30s"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" env:"HTTP_SHUTDOWN_TIMEOUT" env-default:"15s"`
}

type Database struct {
	Host         string `yaml:"host" env:"DB_HOST" env-default:"localhost"`
	Port         string `yaml:"port" env:"DB_PORT" env-default:"5432"`
	User         string `yaml:"user" env:"DB_USER" env-default:"postgres"`
	Password     string `yaml:"password" env:"DB_PASSWORD" env-default:"postgres"`
	Name         string `yaml:"name" env:"DB_NAME" env-default:"control_plane"`
	SSLMode      string `yaml:"ssl_mode" env:"DB_SSL_MODE" env-default:"disable"`
	MaxOpenConns int    `yaml:"max_open_conns" env:"DB_MAX_OPEN_CONNS" env-default:"20"`
	MaxIdleConns int    `yaml:"max_idle_conns" env:"DB_MAX_IDLE_CONNS" env-default:"10"`
}

type NATS struct {
	URL           string        `yaml:"url" env:"NATS_URL" env-default:"nats://localhost:4222"`
	ClientID      string        `yaml:"client_id" env:"NATS_CLIENT_ID" env-default:"control-plane"`
	StreamName    string        `yaml:"stream_name" env:"NATS_STREAM_NAME" env-default:"CALC"`
	AckWait       time.Duration `yaml:"ack_wait" env:"NATS_ACK_WAIT" env-default:"2m"`
	MaxAckPending int           `yaml:"max_ack_pending" env:"NATS_MAX_ACK_PENDING" env-default:"64"`
	Subjects      NATSSubjects  `yaml:"subjects"`
}

type NATSSubjects struct {
	PrepTasks        string `yaml:"prep_tasks" env:"NATS_SUBJECT_PREP_TASKS" env-default:"data-prep.tasks"`
	PrepEvents       string `yaml:"prep_events" env:"NATS_SUBJECT_PREP_EVENTS" env-default:"data-prep.events"`
	CashflowTasks    string `yaml:"cashflow_tasks" env:"NATS_SUBJECT_CASHFLOW_TASKS" env-default:"cashflow.tasks"`
	CashflowEvents   string `yaml:"cashflow_events" env:"NATS_SUBJECT_CASHFLOW_EVENTS" env-default:"cashflow.events"`
	PVTasks          string `yaml:"pv_tasks" env:"NATS_SUBJECT_PV_TASKS" env-default:"pv.tasks"`
	PVEvents         string `yaml:"pv_events" env:"NATS_SUBJECT_PV_EVENTS" env-default:"pv.events"`
	AggregatorTasks  string `yaml:"aggregator_tasks" env:"NATS_SUBJECT_AGGREGATOR_TASKS" env-default:"aggregator.tasks"`
	AggregatorEvents string `yaml:"aggregator_events" env:"NATS_SUBJECT_AGGREGATOR_EVENTS" env-default:"aggregator.events"`
}

type S3 struct {
	Endpoint     string `yaml:"endpoint" env:"S3_ENDPOINT" env-default:"localhost:9000"`
	Region       string `yaml:"region" env:"S3_REGION" env-default:"us-east-1"`
	AccessKey    string `yaml:"access_key" env:"S3_ACCESS_KEY" env-default:"minioadmin"`
	SecretKey    string `yaml:"secret_key" env:"S3_SECRET_KEY" env-default:"minioadmin"`
	UseSSL       bool   `yaml:"use_ssl" env:"S3_USE_SSL" env-default:"false"`
	LakeBucket   string `yaml:"lake_bucket" env:"S3_LAKE_BUCKET" env-default:"data-lake"`
	ReportBucket string `yaml:"report_bucket" env:"S3_REPORT_BUCKET" env-default:"report-store"`
}

type Run struct {
	BatchTargetWeight int64         `yaml:"batch_target_weight" env:"RUN_BATCH_TARGET_WEIGHT" env-default:"50000"`
	MaxBatchesPerRun  int           `yaml:"max_batches_per_run" env:"RUN_MAX_BATCHES_PER_RUN" env-default:"10000"`
	MaxAttempts       int           `yaml:"max_attempts" env:"RUN_MAX_ATTEMPTS" env-default:"3"`
	LeaseTTL          time.Duration `yaml:"lease_ttl" env:"RUN_LEASE_TTL" env-default:"10m"`
	ScanInterval      time.Duration `yaml:"scan_interval" env:"RUN_SCAN_INTERVAL" env-default:"10s"`
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
