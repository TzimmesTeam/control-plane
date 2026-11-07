package httpapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/config"
	"github.com/gin-gonic/gin"
)

const (
	readinessTimeout  = 2 * time.Second
	readHeaderTimeout = 5 * time.Second

	pathHealthz = "/healthz"
	pathReadyz  = "/readyz"

	statusOK       = "ok"
	statusNotReady = "not ready"
	codeInternal   = "internal_error"
)

type ReadinessCheck func(ctx context.Context) error

type Routes interface {
	Register(r gin.IRouter)
}

type App struct {
	server          *http.Server
	shutdownTimeout time.Duration
	log             *slog.Logger
}

func New(log *slog.Logger, cfg config.HTTP, routes Routes, checks map[string]ReadinessCheck) *App {
	if log == nil {
		log = slog.Default()
	}

	gin.SetMode(gin.ReleaseMode)

	engine := gin.New()
	engine.Use(recoveryMiddleware(log), loggingMiddleware(log))

	if routes != nil {
		routes.Register(engine)
	}
	engine.GET(pathHealthz, healthz)
	engine.GET(pathReadyz, gin.WrapF(readyz(log, checks)))
	server := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(cfg.Port)),
		Handler:           engine,
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: readHeaderTimeout,
		WriteTimeout:      cfg.WriteTimeout,
	}

	return &App{server: server, shutdownTimeout: cfg.ShutdownTimeout, log: log}
}

func (a *App) Run() error {
	const op = "httpapp.Run"

	a.log.Info("http server started", slog.String("addr", a.server.Addr))

	if err := a.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s: %w", op, err)
	}

	a.log.Info("http server stopped")

	return nil
}

func (a *App) Stop(ctx context.Context) error {
	const op = "httpapp.Stop"

	if a == nil || a.server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, a.shutdownTimeout)
	defer cancel()

	if err := a.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": statusOK})
}

func readyz(log *slog.Logger, checks map[string]ReadinessCheck) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
		defer cancel()

		w.Header().Set("Content-Type", "application/json")

		for name, check := range checks {
			if err := check(ctx); err != nil {
				log.Warn("readiness check failed", slog.String("dependency", name), slog.Any("error", err))
				w.WriteHeader(http.StatusServiceUnavailable)
				writeBody(log, w, `{"status":"`+statusNotReady+`"}`)
				return
			}
		}

		writeBody(log, w, `{"status":"`+statusOK+`"}`)
	}
}

func writeBody(log *slog.Logger, w http.ResponseWriter, body string) {
	if _, err := w.Write([]byte(body)); err != nil {
		log.Warn("write response failed", slog.Any("error", err))
	}
}

func loggingMiddleware(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		path := c.Request.URL.Path
		attrs := []any{
			slog.String("method", c.Request.Method),
			slog.String("path", path),
			slog.Int("status", c.Writer.Status()),
			slog.Duration("duration", time.Since(start)),
		}

		if path == pathHealthz || path == pathReadyz {
			log.Debug("http request", attrs...)
			return
		}
		log.Info("http request", attrs...)
	}
}

func recoveryMiddleware(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error("panic recovered in http handler",
					slog.String("method", c.Request.Method),
					slog.String("path", c.Request.URL.Path),
					slog.Any("panic", recovered),
				)
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": gin.H{"code": codeInternal, "message": "internal error"},
				})
			}
		}()

		c.Next()
	}
}
