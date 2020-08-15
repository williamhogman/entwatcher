package main

import (
	"encoding/json"
)

type Meta struct {
	UpdatedEntity string `json:"updated_entity"`
}
type Trigger struct {
	Jid           string `json:"jid"`
	Meta          Meta `json:"meta"`
	ActionSource string `json:"action_source"`
	Action       string `json:"action"`
}

func NewTrigger(jid string, updated_entity string, action_id string) *Trigger {
	return &Trigger{
		jid + "/" + action_id,
		Meta{updated_entity},
		"PATH",
		"/entity/" + action_id,
	}
}

func (t *Trigger) AsBytes() ([]byte, error) {
	return json.Marshal(t)
}
