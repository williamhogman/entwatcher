package main

import (
	"encoding/json"
)

type Meta struct {
	UpdatedEntity string `json:"updated_entity"`
}
type Trigger struct {
	Jid           string
	Meta          Meta
	ActionSource string `json:"action_source"`
	Action       string
}

func NewTrigger(jid string, updated_entity string, action_id string) *Trigger {
	return &Trigger{
		jid + "/" + action_id,
		Meta{updated_entity},
		"ENTITY",
		action_id,
	}
}

func (t *Trigger) AsBytes() ([]byte, error) {
	return json.Marshal(t)
}
