package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

type EventHandler interface {
	HandleResult(context.Context, models.Stage, events.Result) error
}
type Broker struct {
	nc        *natsgo.Conn
	js        jetstream.JetStream
	stream    string
	prefix    string
	log       *slog.Logger
	consumers []jetstream.ConsumeContext
}

func New(ctx context.Context, url, stream, prefix string, log *slog.Logger) (*Broker, error) {
	if stream == "" || prefix == "" {
		return nil, fmt.Errorf("NATS stream and prefix are required")
	}
	nc, err := natsgo.Connect(url, natsgo.Name("control-plane"), natsgo.Timeout(5*time.Second), natsgo.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: stream, Subjects: []string{prefix + ".>"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Duplicates: 10 * time.Minute})
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &Broker{nc: nc, js: js, stream: stream, prefix: prefix, log: log}, nil
}
func (b *Broker) PublishTask(ctx context.Context, subject, id string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = b.js.Publish(ctx, subject, data, jetstream.WithMsgID(id))
	return err
}
func (b *Broker) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !b.nc.IsConnected() {
		return fmt.Errorf("NATS disconnected")
	}
	return nil
}
func (b *Broker) Start(ctx context.Context, handler EventHandler) error {
	for _, stage := range []models.Stage{models.Cashflow, models.PV, models.Aggregate} {
		consumer, err := b.js.CreateOrUpdateConsumer(ctx, b.stream, jetstream.ConsumerConfig{
			Durable: "control-plane-" + string(stage), FilterSubject: b.prefix + "." + string(stage) + ".events",
			AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: -1, AckWait: time.Minute, MaxAckPending: 32,
		})
		if err != nil {
			return err
		}
		cc, err := consumer.Consume(func(msg jetstream.Msg) {
			handleCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			err := b.route(handleCtx, handler, stage, msg.Data())
			var settle error
			switch {
			case errors.Is(err, models.ErrInvalid):
				settle = msg.Term()
			case err != nil:
				settle = msg.NakWithDelay(2 * time.Second)
			default:
				settle = msg.Ack()
			}
			if err != nil {
				b.log.Error("handle worker result", "subject", msg.Subject(), "error", err)
			}
			if settle != nil {
				b.log.Error("settle worker result", "error", settle)
			}
		})
		if err != nil {
			return err
		}
		b.consumers = append(b.consumers, cc)
	}
	return nil
}
func (b *Broker) route(ctx context.Context, handler EventHandler, stage models.Stage, data []byte) (err error) {
	defer func() {
		if value := recover(); value != nil {
			b.log.Error("event handler panic", "panic", value, "stack", string(debug.Stack()))
			err = fmt.Errorf("event handler panic: %v", value)
		}
	}()
	var result events.Result
	if err := json.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("%w: malformed event JSON", models.ErrInvalid)
	}
	return handler.HandleResult(ctx, stage, result)
}
func (b *Broker) Stop(ctx context.Context) error {
	for _, cc := range b.consumers {
		cc.Drain()
	}
	for _, cc := range b.consumers {
		select {
		case <-cc.Closed():
		case <-ctx.Done():
			for _, c := range b.consumers {
				c.Stop()
			}
			return ctx.Err()
		}
	}
	return nil
}
func (b *Broker) Close() { b.nc.Close() }
