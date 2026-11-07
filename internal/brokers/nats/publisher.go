package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	duplicatesWindow = 5 * time.Minute
	connectTimeout   = 5 * time.Second
)

type PublisherConfig struct {
	URL        string
	ClientID   string
	StreamName string
	Subjects   []string
}

type Publisher struct {
	nc         *natsgo.Conn
	js         jetstream.JetStream
	streamName string
	log        *slog.Logger
}

func NewPublisher(ctx context.Context, cfg PublisherConfig, log *slog.Logger) (*Publisher, error) {
	const op = "nats.NewPublisher"

	if log == nil {
		log = slog.Default()
	}
	if cfg.URL == "" {
		return nil, fmt.Errorf("%s: url is required", op)
	}
	if cfg.StreamName == "" {
		return nil, fmt.Errorf("%s: stream name is required", op)
	}

	nc, err := natsgo.Connect(cfg.URL,
		natsgo.Name(cfg.ClientID),
		natsgo.Timeout(connectTimeout),
		natsgo.MaxReconnects(-1),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: connect: %w", op, err)
	}

	p, err := initJetStream(ctx, nc, cfg, log)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return p, nil
}

func initJetStream(ctx context.Context, nc *natsgo.Conn, cfg PublisherConfig, log *slog.Logger) (*Publisher, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("create jetstream: %w", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       cfg.StreamName,
		Subjects:   cfg.Subjects,
		Retention:  jetstream.WorkQueuePolicy,
		Storage:    jetstream.FileStorage,
		Duplicates: duplicatesWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("create stream %s: %w", cfg.StreamName, err)
	}

	log.Info("nats stream ready", slog.String("stream", cfg.StreamName), slog.Any("subjects", cfg.Subjects))

	return &Publisher{nc: nc, js: js, streamName: cfg.StreamName, log: log}, nil
}

func (p *Publisher) PublishTask(ctx context.Context, subject, msgID string, msg any) error {
	const op = "nats.PublishTask"

	if p == nil || p.js == nil {
		return fmt.Errorf("%s: publisher is not initialized", op)
	}
	if subject == "" {
		return fmt.Errorf("%s: subject is required", op)
	}
	if msgID == "" {
		return fmt.Errorf("%s: msg id is required", op)
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("%s: marshal message: %w", op, err)
	}

	ack, err := p.js.Publish(ctx, subject, payload, jetstream.WithMsgID(msgID))
	if err != nil {
		return fmt.Errorf("%s: publish to %s: %w", op, subject, err)
	}

	p.log.Debug("task published",
		slog.String("subject", subject),
		slog.String("msg_id", msgID),
		slog.Uint64("seq", ack.Sequence),
		slog.Bool("duplicate", ack.Duplicate),
	)

	return nil
}

func (p *Publisher) JetStream() jetstream.JetStream {
	if p == nil {
		return nil
	}

	return p.js
}

func (p *Publisher) StreamName() string {
	if p == nil {
		return ""
	}

	return p.streamName
}

func (p *Publisher) Ping(_ context.Context) error {
	if p == nil || p.nc == nil {
		return errors.New("nats.Ping: publisher is not initialized")
	}
	if !p.nc.IsConnected() {
		return fmt.Errorf("nats.Ping: connection status %s", p.nc.Status())
	}

	return nil
}

func (p *Publisher) Close() error {
	if p == nil || p.nc == nil {
		return nil
	}

	if err := p.nc.Drain(); err != nil {
		p.nc.Close()
		return fmt.Errorf("nats.Close: drain: %w", err)
	}

	return nil
}
