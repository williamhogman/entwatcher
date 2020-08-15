package main

import (
	"encoding/json"
	"strings"
)

type ActionProperty struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type WatcherEntity struct {
	Properties       []ActionProperty `json:"properties"`
	WildcardTriggers []string `json:"wildcard_triggers"`
}

func (we *WatcherEntity) NamedEntities() []string {
	result := make([]string, 0, len(we.Properties) / 2)
	for _, v := range we.Properties {
		if v.Kind == "ENTITY" {
			result = append(result, v.Value)
		} else if v.Kind == "PATH" {
			entity := strings.TrimPrefix(v.Value, "/entity/")
			result = append(result, entity)
		}
	}
	return result
}

func (we *WatcherEntity) AllWatched() ([]string, []string) {
	return we.NamedEntities(), we.WildcardTriggers
}

type UpdateAction struct {
	Entity WatcherEntity `json:"entity"`
	Name   string `json:"name"`
}


func AllWatched(data []byte) (string, []string, []string, error) {
	action, err := readAction(data)
	if err != nil {
		return "", nil, nil, err
	}
	named, wildcards := action.Entity.AllWatched()
	return action.Name, named, wildcards, nil
}

func readAction(data []byte) (*UpdateAction, error) {
	action := &UpdateAction{}
	err := json.Unmarshal(data, action)
	if err != nil {
		return nil, err
	}
	return action, nil
}
