package main

import (
	"context"
	"fmt"
	nats "github.com/nats-io/nats.go"
	"go.uber.org/fx"
	"net/url"
)

type Subs = *map[string]nats.MsgHandler

func SetupSubscriptions(subs Subs, nc *nats.Conn) error {
	for k, v := range *subs {
		if _, err := nc.Subscribe(k, v); err != nil {
			return err
		}
	}
	return nil
}

func NewNats(lc fx.Lifecycle, c *Config) (*nats.Conn, error) {
	nc, err := nats.Connect(c.NatsUrl)

	if err != nil {
		if err, ok := err.(*url.Error); ok {
			return nil, fmt.Errorf("NATS_URL is of an incorrect format: %w", err)
		}
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return nc.Drain()
		},
	})

	return nc, nil
}
