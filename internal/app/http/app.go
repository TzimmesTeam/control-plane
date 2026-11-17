package httpapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/TzimmesTeam/control-plane/internal/config"
)

type Routes interface{ Register(gin.IRouter) }
type ReadinessCheck func(context.Context) error
type App struct{ server *http.Server }

func New(log *slog.Logger, cfg config.HTTP, routes Routes, checks map[string]ReadinessCheck) *App {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		defer func() {
			if value := recover(); value != nil {
				log.Error("HTTP panic", "panic", value)
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			}
		}()
		c.Next()
	})
	routes.Register(engine)
	engine.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })
	engine.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		for name, check := range checks {
			if err := check(ctx); err != nil {
				log.Warn("dependency not ready", "dependency", name, "error", err)
				c.Status(http.StatusServiceUnavailable)
				return
			}
		}
		c.Status(http.StatusOK)
	})
	return &App{server: &http.Server{Addr: fmt.Sprintf(":%d", cfg.Port), Handler: engine, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute}}
}
func (a *App) Run() error {
	err := a.server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (a *App) Stop(ctx context.Context) error { return a.server.Shutdown(ctx) }
