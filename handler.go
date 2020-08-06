package main

import (
	"bytes"
	"context"
	nats "github.com/nats-io/nats.go"
	"log"
	"strings"
	"time"
)

type Handler struct {
	storage Storage
	nats    *nats.Conn
}

const SEP = "/"
const ActionTopic = "conthesis.action.TriggerAsyncAction"
const NotifyUpdateAccepted = "entity-updates-v1.accepted"

func (handler *Handler) Action(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	name, namedEnts, wildcards, err := AllWatched(m.Data)
	if err != nil {
		log.Printf("Failed to decode message %v", err)
		m.Respond([]byte(""))
		return
	}
	log.Printf("Adding %v exact and %v wildcards", len(namedEnts), len(wildcards))
	err = handler.storage.AddWatches(ctx, name, namedEnts, wildcards)

	if err != nil {
		log.Printf("Failed to store watches %v", err)
		m.Respond([]byte(""))
		return
	}
}

func (handler *Handler) Update(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	i := bytes.IndexByte(m.Data, 0)
	if i == -1 {
		log.Printf("Invalid format separator missing")
		return
	}
	entity := string(m.Data[:i])

	parts := strings.Split(SEP, entity)
	butLast := parts[:len(parts)-1]
	last := parts[len(parts)-1:]

	matches, err := handler.storage.Match(ctx, butLast, last)
	if err != nil {
		log.Printf("Failed getting matches %v", err)
		return
	}
	allOk := true
	mData := string(m.Data)
	log.Printf("n=%v entity=%v", len(matches), entity)
	for _, match := range matches {
		data, err := NewTrigger(mData, entity, match).AsBytes()
		log.Printf("DATA: %v", string(data))
		if err != nil {
			log.Printf("Error marshaling data %v")
		}
		_, err = handler.nats.RequestWithContext(ctx, ActionTopic, data)
		if err != nil {
			log.Printf("Unable to publish message")
			allOk = false
		}
	}
	if allOk {
		err = handler.nats.Publish(NotifyUpdateAccepted, m.Data)
		if err != nil {
			log.Printf("Unable to respond to original message")
		}
	}
}

func NewHandler(storage Storage, nats *nats.Conn) *Handler {
	return &Handler{storage, nats}
}
