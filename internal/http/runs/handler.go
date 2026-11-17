package runs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/service"
)

type RunService interface {
	CreateRun(context.Context, service.CreateRunInput) (*models.Run, error)
	Prepared(context.Context, service.CreateRunInput) (*models.Run, error)
	GetRun(context.Context, string) (*models.Run, error)
	ListRunBatches(context.Context, string) ([]models.Batch, error)
	CancelRun(context.Context, string) (*models.Run, error)
}
type Handler struct {
	log *slog.Logger
	svc RunService
}

func NewHandler(log *slog.Logger, svc RunService) *Handler { return &Handler{log: log, svc: svc} }
func (h *Handler) Register(router gin.IRouter) {
	router.POST("/v1/runs", h.create)
	router.POST("/snapshots/prepared", h.prepared)
	router.GET("/v1/runs/:run_id", h.get)
	router.GET("/v1/runs/:run_id/batches", h.batches)
	router.POST("/v1/runs/:run_id/cancel", h.cancel)
}
func decode(w http.ResponseWriter, r *http.Request, value any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return models.ErrInvalid
	}
	return nil
}
func (h *Handler) create(c *gin.Context) {
	var in service.CreateRunInput
	if err := decode(c.Writer, c.Request, &in); err != nil {
		h.reply(c, nil, models.ErrInvalid)
		return
	}
	run, err := h.svc.CreateRun(c.Request.Context(), in)
	h.reply(c, run, err)
}
func (h *Handler) prepared(c *gin.Context) {
	var in service.CreateRunInput
	if err := decode(c.Writer, c.Request, &in); err != nil {
		h.reply(c, nil, models.ErrInvalid)
		return
	}
	run, err := h.svc.Prepared(c.Request.Context(), in)
	h.reply(c, run, err)
}
func (h *Handler) get(c *gin.Context) {
	run, err := h.svc.GetRun(c.Request.Context(), c.Param("run_id"))
	h.reply(c, run, err)
}
func (h *Handler) batches(c *gin.Context) {
	batches, err := h.svc.ListRunBatches(c.Request.Context(), c.Param("run_id"))
	h.reply(c, batches, err)
}
func (h *Handler) cancel(c *gin.Context) {
	run, err := h.svc.CancelRun(c.Request.Context(), c.Param("run_id"))
	h.reply(c, run, err)
}
func (h *Handler) reply(c *gin.Context, value any, err error) {
	status := http.StatusOK
	if err != nil {
		message := err.Error()
		switch {
		case errors.Is(err, models.ErrInvalid):
			status = http.StatusBadRequest
		case errors.Is(err, models.ErrNotFound):
			status = http.StatusNotFound
		case errors.Is(err, models.ErrConflict):
			status = http.StatusConflict
		default:
			status = http.StatusInternalServerError
			message = "internal error"
			h.log.Error("HTTP request failed", "error", err)
		}
		value = map[string]string{"error": message}
	}
	if run, ok := value.(*models.Run); ok && run != nil {
		copy := *run
		copy.PlanningToken = ""
		value = &copy
	}
	c.JSON(status, value)
}
