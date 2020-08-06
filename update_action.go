package main

import (
	"encoding/json"
	"strings"
)

type ActionProperty struct {
	kind  string
	value string
}

type WatcherEntity struct {
	Properties       []ActionProperty
	WildcardTriggers []string `json:"wildcard_triggers"`
}

func (we *WatcherEntity) NamedEntities() []string {
	var result []string
	for _, v := range we.Properties {
		if v.kind == "ENTITY" {
			result = append(result, v.value)
		} else if v.kind == "PATH" {
			entity := strings.TrimPrefix(v.value, "/entity/")
			result = append(result, entity)
		}
	}
	return result
}

func (we *WatcherEntity) AllWatched() ([]string, []string) {
	return we.NamedEntities(), we.WildcardTriggers
}

type UpdateAction struct {
	entity WatcherEntity
	name   string
}


func AllWatched(data []byte) (string, []string, []string, error) {
	action, err := readAction(data)
	if err != nil {
		return "", nil, nil, err
	}
	named, wildcards := action.entity.AllWatched()
	return action.name, named, wildcards, nil
}

func readAction(data []byte) (*UpdateAction, error) {
	action := &UpdateAction{}
	err := json.Unmarshal(data, action)
	if err != nil {
		return nil, err
	}
	return action, nil
}
