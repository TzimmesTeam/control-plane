package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/TzimmesTeam/control-plane/internal/app"
	"github.com/TzimmesTeam/control-plane/internal/config"
)

func main() {
	cfg := config.MustLoad()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	application, err := app.New(context.Background(), cfg, log)
	if err != nil {
		log.Error("failed to init app", slog.Any("error", err))
		os.Exit(1)
	}

	log.Info("application initialized")

	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	stopped := make(chan struct{})

	go func() {
		defer close(stopped)

		<-stopCtx.Done()
		log.Info("shutdown signal received")

		if err := application.Stop(context.WithoutCancel(stopCtx)); err != nil {
			log.Error("failed to stop app", slog.Any("error", err))
			return
		}

		log.Info("application stopped")
	}()

	if err := application.Run(context.Background()); err != nil {
		log.Error("application run failed", slog.Any("error", err))
		os.Exit(1)
	}

	<-stopped
}
