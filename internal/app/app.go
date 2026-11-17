package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	httpapp "github.com/TzimmesTeam/control-plane/internal/app/http"
	broker "github.com/TzimmesTeam/control-plane/internal/brokers/nats"
	"github.com/TzimmesTeam/control-plane/internal/config"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/http/runs"
	"github.com/TzimmesTeam/control-plane/internal/repository/postgres"
	"github.com/TzimmesTeam/control-plane/internal/service"
	"github.com/TzimmesTeam/control-plane/internal/storage/s3"
)

type App struct {
	http     *httpapp.App
	repo     *postgres.Repository
	broker   *broker.Broker
	service  *service.Service
	shutdown time.Duration
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

func New(ctx context.Context, cfg *config.Config, log *slog.Logger) (*App, error) {
	repo, err := postgres.New(ctx, cfg.Database.DSN)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			_ = repo.Close()
		}
	}()
	if err := repo.CheckSchema(ctx); err != nil {
		return nil, err
	}
	store, err := s3.New(s3.Config{
		Endpoint:        cfg.S3.Endpoint,
		Region:          cfg.S3.Region,
		AccessKey:       cfg.S3.AccessKey,
		SecretKey:       cfg.S3.SecretKey,
		UseSSL:          cfg.S3.UseSSL,
		CuratedBucket:   cfg.S3.CuratedBucket,
		ArtifactsBucket: cfg.S3.ArtifactsBucket,
		PreparedPrefix:  cfg.S3.PreparedPrefix,
		PreparedKey:     cfg.S3.PreparedKey,
	})
	if err != nil {
		return nil, err
	}
	messaging, err := broker.New(ctx, cfg.NATS.URL, cfg.NATS.Stream, cfg.NATS.Prefix, log)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			messaging.Close()
		}
	}()
	subjects := map[models.Stage]string{}
	for _, stage := range []models.Stage{models.Cashflow, models.PV, models.Aggregate} {
		subjects[stage] = cfg.NATS.Prefix + "." + string(stage) + ".tasks"
	}
	svc, err := service.NewService(log, repo, store, messaging, service.Settings{
		BatchSize:          cfg.Run.BatchSize,
		MaxBatches:         cfg.Run.MaxBatches,
		MaxAttempts:        cfg.Run.MaxAttempts,
		LeaseTTL:           cfg.Run.LeaseTTL,
		ScanInterval:       cfg.Run.ScanInterval,
		PublishTimeout:     cfg.Run.PublishTimeout,
		ArtifactsBucket:    cfg.S3.ArtifactsBucket,
		CuratedBucket:      cfg.S3.CuratedBucket,
		TaskSubjects:       subjects,
		DefaultPortfolioID: cfg.Run.DefaultPortfolioID,
		DefaultReportDate:  cfg.Run.DefaultReportDate,
		DefaultCurvesURI:   cfg.Run.DefaultCurvesURI,
	})
	if err != nil {
		return nil, err
	}
	server := httpapp.New(log, cfg.HTTP, runs.NewHandler(log, svc), map[string]httpapp.ReadinessCheck{"postgres": repo.Ping, "nats": messaging.Ping})
	success = true
	return &App{http: server, repo: repo, broker: messaging, service: svc, shutdown: cfg.HTTP.ShutdownTimeout, stop: make(chan struct{}), stopped: make(chan struct{})}, nil
}
func (a *App) Run(ctx context.Context) error {
	defer close(a.stopped)
	defer a.repo.Close()
	defer a.broker.Close()
	bg, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := a.broker.Start(bg, a.service); err != nil {
		stopCtx, stop := context.WithTimeout(context.Background(), a.shutdown)
		defer stop()
		_ = a.broker.Stop(stopCtx)
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.service.Run(bg)
	}()
	serverErr := make(chan error, 1)
	go func() { serverErr <- a.http.Run() }()
	var result error
	select {
	case <-ctx.Done():
	case <-a.stop:
	case result = <-serverErr:
	}
	shutdownCtx, stop := context.WithTimeout(context.Background(), a.shutdown)
	defer stop()
	cancel()
	httpErr := a.http.Stop(shutdownCtx)
	brokerErr := a.broker.Stop(shutdownCtx)
	wg.Wait()
	if result != nil {
		return fmt.Errorf("HTTP server: %w", result)
	}
	return errors.Join(httpErr, brokerErr)
}

func (a *App) Stop(ctx context.Context) error {
	a.stopOnce.Do(func() { close(a.stop) })
	select {
	case <-a.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
