package main

import (
	nats "github.com/nats-io/nats.go"
	"go.uber.org/fx"
)

func NewSubscriptions(h *Handler) Subs {
	return &map[string]nats.MsgHandler{
		"conthesis.action.entwatcher.UpdateWatchEntity": h.Action,
		"entity-updates-v1": h.Update,
	}
}

func main() {
	fx.New(
		fx.Provide(
			NewConfig,
			NewSubscriptions,
			NewHandler,
			NewNats,
			NewStorage,
		),
		fx.Invoke(SetupSubscriptions),
	).Run()
}
