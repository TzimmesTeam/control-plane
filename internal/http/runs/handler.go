package runs

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/TzimmesTeam/control-plane/internal/service"
	"github.com/gin-gonic/gin"
)

const (
	maxRequestBody = 1 << 20

	pathRunID   = "run_id"
	queryStage  = "stage"
	queryStatus = "status"

	codeInvalidRequest = "invalid_request"
	codeRunNotFound    = "run_not_found"
	codeRunFinished    = "run_finished"
	codeInternal       = "internal_error"
)

type RunService interface {
	CreateRun(ctx context.Context, in service.CreateRunInput) (*models.Run, error)
	GetRun(ctx context.Context, runID string) (*models.RunDetails, error)
	ListRunBatches(ctx context.Context, runID string, filter models.BatchStageFilter) ([]*models.BatchDetails, error)
	CancelRun(ctx context.Context, runID string) (*models.RunDetails, error)
}

type Handler struct {
	log *slog.Logger
	svc RunService
}

func NewHandler(log *slog.Logger, svc RunService) *Handler {
	if log == nil {
		log = slog.Default()
	}

	return &Handler{log: log, svc: svc}
}

func (h *Handler) Register(r gin.IRouter) {
	v1 := r.Group("/v1/runs")
	v1.POST("", h.CreateRun)
	v1.GET("/:"+pathRunID, h.GetRun)
	v1.GET("/:"+pathRunID+"/batches", h.ListBatches)
	v1.POST("/:"+pathRunID+"/cancel", h.CancelRun)
}

func (h *Handler) CreateRun(c *gin.Context) {
	var req CreateRunRequest

	dec := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, maxRequestBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(c, http.StatusBadRequest, codeInvalidRequest, "invalid json body: "+err.Error())
		return
	}

	run, err := h.svc.CreateRun(c.Request.Context(), service.CreateRunInput{
		PortfolioID: req.PortfolioID,
		ReportDttm:  req.ReportDttm,
		SourceURI:   req.SourceURI,
		CurvesURI:   req.CurvesURI,
	})
	if err != nil {
		h.handleServiceError(c, err)
		return
	}

	c.JSON(http.StatusAccepted, CreateRunResponse{RunID: run.RunID, Status: string(run.Status)})
}

func (h *Handler) GetRun(c *gin.Context) {
	details, err := h.svc.GetRun(c.Request.Context(), c.Param(pathRunID))
	if err != nil {
		h.handleServiceError(c, err)
		return
	}

	c.JSON(http.StatusOK, toRunResponse(details))
}

func (h *Handler) ListBatches(c *gin.Context) {
	filter := models.BatchStageFilter{
		Stage:  models.Stage(c.Query(queryStage)),
		Status: models.BatchStatus(c.Query(queryStatus)),
	}

	batches, err := h.svc.ListRunBatches(c.Request.Context(), c.Param(pathRunID), filter)
	if err != nil {
		h.handleServiceError(c, err)
		return
	}

	c.JSON(http.StatusOK, toBatchesResponse(batches))
}

func (h *Handler) CancelRun(c *gin.Context) {
	details, err := h.svc.CancelRun(c.Request.Context(), c.Param(pathRunID))
	if err != nil {
		h.handleServiceError(c, err)
		return
	}

	c.JSON(http.StatusOK, toRunResponse(details))
}

func (h *Handler) handleServiceError(c *gin.Context, err error) {
	var validationErr *service.ValidationError

	switch {
	case errors.As(err, &validationErr):
		writeError(c, http.StatusBadRequest, codeInvalidRequest, validationErr.Error())
	case errors.Is(err, models.ErrRunNotFound):
		writeError(c, http.StatusNotFound, codeRunNotFound, "run not found")
	case errors.Is(err, models.ErrRunFinished):
		writeError(c, http.StatusConflict, codeRunFinished, "run already finished")
	default:
		h.log.Error("request failed", slog.String("path", c.FullPath()), slog.Any("error", err))
		writeError(c, http.StatusInternalServerError, codeInternal, "internal error")
	}
}

func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, ErrorResponse{Error: ErrorBody{Code: code, Message: message}})
}
