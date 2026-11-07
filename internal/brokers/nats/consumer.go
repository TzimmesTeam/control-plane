package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/TzimmesTeam/control-plane/internal/domain/events"
	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	durablePrefix   = "control-plane-"
	durableSuffix   = "-events"
	maxLoggedBody   = 1024
	eventMaxDeliver = 3
	eventNakDelay   = 2 * time.Second
)

type EventHandler interface {
	HandleBatchEvent(ctx context.Context, stage models.Stage, event events.BatchEvent) error
	HandleAggregateEvent(ctx context.Context, event events.AggregateEvent) error
}

type ConsumerConfig struct {
	StreamName    string
	AckWait       time.Duration
	MaxAckPending int
	StageSubjects map[models.Stage]string
}

type Consumer struct {
	js       jetstream.JetStream
	cfg      ConsumerConfig
	handler  EventHandler
	log      *slog.Logger
	contexts []jetstream.ConsumeContext
}

func NewConsumer(log *slog.Logger, js jetstream.JetStream, cfg ConsumerConfig, handler EventHandler) (*Consumer, error) {
	const op = "nats.NewConsumer"

	if log == nil {
		log = slog.Default()
	}
	if js == nil {
		return nil, fmt.Errorf("%s: jetstream is nil", op)
	}
	if handler == nil {
		return nil, fmt.Errorf("%s: handler is nil", op)
	}
	if cfg.StreamName == "" {
		return nil, fmt.Errorf("%s: stream name is required", op)
	}

	return &Consumer{js: js, cfg: cfg, handler: handler, log: log}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	const op = "nats.Consumer.Start"

	if c == nil || c.js == nil {
		return fmt.Errorf("%s: consumer is not initialized", op)
	}

	for stage, subject := range c.cfg.StageSubjects {
		if err := c.subscribe(ctx, stage, subject); err != nil {
			c.Stop()
			return fmt.Errorf("%s: %w", op, err)
		}
	}

	return nil
}

func (c *Consumer) subscribe(ctx context.Context, stage models.Stage, subject string) error {
	cons, err := c.js.CreateOrUpdateConsumer(ctx, c.cfg.StreamName, jetstream.ConsumerConfig{
		Durable:       durablePrefix + string(stage) + durableSuffix,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    eventMaxDeliver,
		AckWait:       c.cfg.AckWait,
		MaxAckPending: c.cfg.MaxAckPending,
	})
	if err != nil {
		return fmt.Errorf("create consumer for %s: %w", subject, err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		c.handle(ctx, stage, msg)
	})
	if err != nil {
		return fmt.Errorf("consume %s: %w", subject, err)
	}

	c.contexts = append(c.contexts, cc)
	c.log.Info("nats consumer started", slog.String("subject", subject), slog.String("stage", string(stage)))

	return nil
}

func (c *Consumer) handle(ctx context.Context, stage models.Stage, msg jetstream.Msg) {
	var err error
	if stage == models.StageAggregate {
		err = c.routeAggregateEvent(ctx, msg)
	} else {
		err = c.routeBatchEvent(ctx, stage, msg)
	}

	switch {
	case errors.Is(err, errInvalidPayload):
		c.settle(msg, "term", msg.Term())
	case err != nil:
		c.settle(msg, "nak", msg.NakWithDelay(eventNakDelay))
	default:
		c.settle(msg, "ack", msg.Ack())
	}
}

var errInvalidPayload = errors.New("invalid event payload")

func (c *Consumer) settle(msg jetstream.Msg, action string, err error) {
	if err != nil {
		c.log.Error("event "+action+" failed", slog.String("subject", msg.Subject()), slog.Any("error", err))
	}
}

func (c *Consumer) routeBatchEvent(ctx context.Context, stage models.Stage, msg jetstream.Msg) error {
	var event events.BatchEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		c.logInvalidPayload(msg, err)
		return errInvalidPayload
	}

	c.log.Debug("batch event received",
		slog.String("run_id", event.RunID),
		slog.String("batch_id", event.BatchID),
		slog.String("stage", string(stage)),
		slog.Int("attempt", event.Attempt),
		slog.String("status", event.Status),
	)

	if err := c.handler.HandleBatchEvent(ctx, stage, event); err != nil {
		c.log.Error("batch event handling failed",
			slog.String("run_id", event.RunID),
			slog.String("batch_id", event.BatchID),
			slog.String("stage", string(stage)),
			slog.Int("attempt", event.Attempt),
			slog.Any("error", err),
		)
		return err
	}

	return nil
}

func (c *Consumer) routeAggregateEvent(ctx context.Context, msg jetstream.Msg) error {
	var event events.AggregateEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		c.logInvalidPayload(msg, err)
		return errInvalidPayload
	}

	c.log.Debug("aggregate event received",
		slog.String("run_id", event.RunID),
		slog.Int("attempt", event.Attempt),
		slog.String("status", event.Status),
	)

	if err := c.handler.HandleAggregateEvent(ctx, event); err != nil {
		c.log.Error("aggregate event handling failed",
			slog.String("run_id", event.RunID),
			slog.Int("attempt", event.Attempt),
			slog.Any("error", err),
		)
		return err
	}

	return nil
}

func (c *Consumer) logInvalidPayload(msg jetstream.Msg, err error) {
	body := msg.Data()
	if len(body) > maxLoggedBody {
		body = body[:maxLoggedBody]
	}

	c.log.Warn("invalid event payload",
		slog.String("subject", msg.Subject()),
		slog.String("body", string(body)),
		slog.Any("error", err),
	)
}

func (c *Consumer) Stop() {
	if c == nil {
		return
	}

	for _, cc := range c.contexts {
		cc.Stop()
	}
	c.contexts = nil
}
