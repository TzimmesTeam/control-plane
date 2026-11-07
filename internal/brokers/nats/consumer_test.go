package nats

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeMsg struct {
	jetstream.Msg
	data    []byte
	settled string
}

func (m *fakeMsg) Data() []byte    { return m.data }
func (m *fakeMsg) Subject() string { return "cashflow.events" }
func (m *fakeMsg) Ack() error      { m.settled = "ack"; return nil }
func (m *fakeMsg) Term() error     { m.settled = "term"; return nil }
func (m *fakeMsg) NakWithDelay(time.Duration) error {
	m.settled = "nak"
	return nil
}

type fakeHandler struct {
	batch func() error
}

func (h fakeHandler) HandleBatchEvent(context.Context, models.Stage, events.BatchEvent) error {
	return h.batch()
}

func (h fakeHandler) HandleAggregateEvent(context.Context, events.AggregateEvent) error {
	return nil
}

func TestHandleSettlesMessage(t *testing.T) {
	const validEvent = `{"run_id":"r","batch_id":"b","attempt":1,"status":"success"}`

	tests := []struct {
		name    string
		body    string
		handler func() error
		want    string
	}{
		{name: "success acks", body: validEvent, handler: func() error { return nil }, want: "ack"},
		{name: "handler error naks", body: validEvent, handler: func() error { return errors.New("db down") }, want: "nak"},
		{name: "invalid payload terms", body: `{`, handler: func() error { return nil }, want: "term"},
		{name: "panic terms", body: validEvent, handler: func() error { panic("boom") }, want: "term"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Consumer{
				handler: fakeHandler{batch: tt.handler},
				log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			msg := &fakeMsg{data: []byte(tt.body)}

			c.handle(context.Background(), models.StageCashflow, msg)

			if msg.settled != tt.want {
				t.Errorf("settled = %q, want %q", msg.settled, tt.want)
			}
		})
	}
}
