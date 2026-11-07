package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	httpapp "github.com/TzimmesTeam/control-plane/internal/app/http"
	natsbroker "github.com/TzimmesTeam/control-plane/internal/brokers/nats"
	"github.com/TzimmesTeam/control-plane/internal/config"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/http/runs"
	"github.com/TzimmesTeam/control-plane/internal/repository/postgres"
	"github.com/TzimmesTeam/control-plane/internal/service"
	"github.com/TzimmesTeam/control-plane/internal/storage/s3"
)

const (
	recoveryTimeout = 2 * time.Minute

	streamSubjectPrep       = "data-prep.>"
	streamSubjectCashflow   = "cashflow.>"
	streamSubjectPV         = "pv.>"
	streamSubjectAggregator = "aggregator.>"
)

type App struct {
	HTTP      *httpapp.App
	repo      *postgres.Repository
	publisher *natsbroker.Publisher
	consumer  *natsbroker.Consumer
	service   *service.Service
	log       *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(ctx context.Context, cfg *config.Config, log *slog.Logger) (*App, error) {
	const op = "App.New"

	if cfg == nil {
		return nil, fmt.Errorf("%s: config is nil", op)
	}
	if log == nil {
		log = slog.Default()
	}

	repo, err := postgres.New(ctx, postgres.Config{
		DSN:          makeDSN(cfg.Database),
		MaxOpenConns: cfg.Database.MaxOpenConns,
		MaxIdleConns: cfg.Database.MaxIdleConns,
	}, log)
	if err != nil {
		return nil, fmt.Errorf("%s: init repository: %w", op, err)
	}

	store, err := s3.New(ctx, s3.Config{
		Endpoint:  cfg.S3.Endpoint,
		Region:    cfg.S3.Region,
		AccessKey: cfg.S3.AccessKey,
		SecretKey: cfg.S3.SecretKey,
		UseSSL:    cfg.S3.UseSSL,
		Buckets:   []string{cfg.S3.LakeBucket, cfg.S3.ReportBucket},
	}, log)
	if err != nil {
		closeRepo(log, repo)
		return nil, fmt.Errorf("%s: init s3: %w", op, err)
	}

	publisher, err := natsbroker.NewPublisher(ctx, natsbroker.PublisherConfig{
		URL:        cfg.NATS.URL,
		ClientID:   cfg.NATS.ClientID,
		StreamName: cfg.NATS.StreamName,
		Subjects:   []string{streamSubjectPrep, streamSubjectCashflow, streamSubjectPV, streamSubjectAggregator},
	}, log)
	if err != nil {
		closeRepo(log, repo)
		return nil, fmt.Errorf("%s: init nats publisher: %w", op, err)
	}

	a, err := assemble(cfg, log, repo, store, publisher)
	if err != nil {
		closePublisher(log, publisher)
		closeRepo(log, repo)
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return a, nil
}

func assemble(
	cfg *config.Config,
	log *slog.Logger,
	repo *postgres.Repository,
	store *s3.Client,
	publisher *natsbroker.Publisher,
) (*App, error) {
	subjects := cfg.NATS.Subjects

	svc, err := service.NewService(log, repo, store, publisher, service.Settings{
		LakeBucket:        cfg.S3.LakeBucket,
		ReportBucket:      cfg.S3.ReportBucket,
		BatchTargetWeight: cfg.Run.BatchTargetWeight,
		MaxBatchesPerRun:  cfg.Run.MaxBatchesPerRun,
		MaxAttempts:       cfg.Run.MaxAttempts,
		LeaseTTL:          cfg.Run.LeaseTTL,
		ScanInterval:      cfg.Run.ScanInterval,
		TaskSubjects: map[models.Stage]string{
			models.StagePrep:      subjects.PrepTasks,
			models.StageCashflow:  subjects.CashflowTasks,
			models.StagePV:        subjects.PVTasks,
			models.StageAggregate: subjects.AggregatorTasks,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("init service: %w", err)
	}

	consumer, err := natsbroker.NewConsumer(log, publisher.JetStream(), natsbroker.ConsumerConfig{
		StreamName:    publisher.StreamName(),
		AckWait:       cfg.NATS.AckWait,
		MaxAckPending: cfg.NATS.MaxAckPending,
		StageSubjects: map[models.Stage]string{
			models.StagePrep:      subjects.PrepEvents,
			models.StageCashflow:  subjects.CashflowEvents,
			models.StagePV:        subjects.PVEvents,
			models.StageAggregate: subjects.AggregatorEvents,
		},
	}, svc)
	if err != nil {
		return nil, fmt.Errorf("init nats consumer: %w", err)
	}

	httpServer := httpapp.New(log, cfg.HTTP, runs.NewHandler(log, svc), map[string]httpapp.ReadinessCheck{
		"postgres": repo.Ping,
		"nats":     publisher.Ping,
	})

	return &App{
		HTTP:      httpServer,
		repo:      repo,
		publisher: publisher,
		consumer:  consumer,
		service:   svc,
		log:       log,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	const op = "App.Run"

	if a == nil || a.HTTP == nil || a.service == nil {
		return errors.New("app.Run: app is not initialized")
	}

	bgCtx, cancel := context.WithCancel(ctx)
	a.mu.Lock()
	a.cancel = cancel
	a.mu.Unlock()

	if err := a.consumer.Start(bgCtx); err != nil {
		return fmt.Errorf("%s: start consumer: %w", op, err)
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.service.Run(bgCtx)
	}()

	recoverCtx, cancelRecover := context.WithTimeout(bgCtx, recoveryTimeout)
	defer cancelRecover()

	if err := a.service.Recover(recoverCtx); err != nil {
		a.log.Error("recovery incomplete", slog.Any("error", err))
	}

	a.log.Info("starting http app")

	return a.HTTP.Run()
}

func (a *App) Stop(ctx context.Context) error {
	const op = "App.Stop"

	if a == nil {
		return nil
	}

	var errs []error

	a.log.Info("stopping http app")
	if a.HTTP != nil {
		if err := a.HTTP.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: stop http: %w", op, err))
		}
	}

	a.consumer.Stop()

	a.mu.Lock()
	if a.cancel != nil {
		a.cancel()
	}
	a.mu.Unlock()
	a.wg.Wait()

	if err := a.publisher.Close(); err != nil {
		errs = append(errs, fmt.Errorf("%s: close nats: %w", op, err))
	}
	if err := a.repo.Close(); err != nil {
		errs = append(errs, fmt.Errorf("%s: close repository: %w", op, err))
	}

	return errors.Join(errs...)
}

func closeRepo(log *slog.Logger, repo *postgres.Repository) {
	if err := repo.Close(); err != nil {
		log.Error("failed to close repository", slog.Any("error", err))
	}
}

func closePublisher(log *slog.Logger, publisher *natsbroker.Publisher) {
	if err := publisher.Close(); err != nil {
		log.Error("failed to close nats publisher", slog.Any("error", err))
	}
}

func makeDSN(cfg config.Database) string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host,
		cfg.Port,
		cfg.User,
		cfg.Password,
		cfg.Name,
		cfg.SSLMode,
	)
}
