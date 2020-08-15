package main

import (
	"bytes"
	"context"
	"log"
	"strings"
	"time"
	"encoding/base64"
	"github.com/nats-io/nats.go"
)

type Handler struct {
	storage Storage
	nats    *nats.Conn
}

const SEP = '/'
const ActionTopic = "conthesis.action.TriggerAsyncAction"
const NotifyUpdateAccepted = "entity-updates-v1.accepted"
const ConfirmTopicBase = "conthesis.entwatcher.confirmations."

func createConfirmationTopic(confirmationToken []byte) string {
	bfr := bytes.Buffer{}
	bfr.Grow(base64.RawURLEncoding.EncodedLen(len(confirmationToken)) + len(ConfirmTopicBase))
	bfr.WriteString(ConfirmTopicBase)
	enc := base64.NewEncoder(base64.RawURLEncoding, &bfr)
	_, err := enc.Write(confirmationToken)
	if err != nil {
		panic(err)
	}
	return bfr.String()
}

func (handler *Handler) Action(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	name, namedEnts, wildcards, err := AllWatched(m.Data)
	if err != nil {
		log.Printf("Failed to decode message %v", err)
		m.Respond([]byte("{}"))
		return
	}
	err = handler.storage.AddWatches(ctx, name, namedEnts, wildcards)

	if err != nil {
		log.Printf("Failed to store watches %v", err)
		m.Respond([]byte("{}"))
		return
	}
	m.Respond([]byte("{}"))
}

func splitParts(x string) []string {
	res := make([]string, 0, len(x) / 5)
	offset := 0
	for {
		i := strings.IndexRune(x[offset:], SEP)
		if i == -1 {
			break
		}
		res = append(res, x[:offset + i])
		offset += i + 1
	}
	res = append(res, x)
	return res
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

	parts := splitParts(entity)
	butLast := parts[:len(parts)-1]
	last := parts[len(parts)-1:]

	matches, err := handler.storage.Match(ctx, last, butLast)
	if err != nil {
		log.Printf("Failed getting matches %v", err)
		return
	}
	allOk := true
	mData := string(m.Data)
	if len(matches) == 1 {
		match := matches[0]
		data, err := NewTrigger(mData, entity, match).AsBytes()
		if err != nil {
			log.Printf("Error marshaling data %v", err)
			return
		}
		err = handler.nats.PublishRequest(ActionTopic, createConfirmationTopic(m.Data), data)
		if err != nil {
			log.Printf("Unable to publish message")
			allOk = false
		}
	} else {
		for _, match := range matches {
			data, err := NewTrigger(mData, entity, match).AsBytes()
			if err != nil {
				log.Printf("Error marshaling data %v", err)
				continue
			}
			_, err = handler.nats.RequestWithContext(ctx, ActionTopic, data)
			if err != nil {
				log.Printf("Unable to publish message %v", err)
				allOk = false
			}
		}
	}
	if allOk {
		err = handler.nats.Publish(NotifyUpdateAccepted, m.Data)
		if err != nil {
			log.Printf("Unable to respond to original message")
		}
	}
}

func (handler *Handler) HandleConfirm(m *nats.Msg) {
	decodedData, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(m.Subject, ConfirmTopicBase))
	if err != nil {
		log.Printf("Failed to decode confirmation data: %v", err)
		return
	}
	if err := handler.nats.Publish(NotifyUpdateAccepted, decodedData); err != nil {
		log.Printf("Failed to publish %v", err)
		return
	}
}

func NewHandler(storage Storage, nats *nats.Conn) *Handler {
	return &Handler{storage, nats}
}
