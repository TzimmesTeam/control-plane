package nats

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

type fakeHandler struct {
	panic bool
	err   error
	stage models.Stage
}

func (f *fakeHandler) HandleResult(_ context.Context, stage models.Stage, _ events.Result) error {
	if f.panic {
		panic("failure")
	}
	f.stage = stage
	return f.err
}
func TestResultRoutingAndRecovery(t *testing.T) {
	broker := &Broker{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	handler := &fakeHandler{}
	if err := broker.route(context.Background(), handler, models.PV, []byte(`{"task_id":"task","status":"success"}`)); err != nil || handler.stage != models.PV {
		t.Fatal("stage not routed", err)
	}
	if err := broker.route(context.Background(), handler, models.PV, []byte(`{`)); !errors.Is(err, models.ErrInvalid) {
		t.Fatal("malformed JSON must be terminated")
	}
	handler.panic = true
	if err := broker.route(context.Background(), handler, models.PV, []byte(`{}`)); err == nil || errors.Is(err, models.ErrInvalid) {
		t.Fatal("panic must be contained and retriable")
	}
}
