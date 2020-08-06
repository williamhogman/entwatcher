package main

import (
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	NatsUrl       string `split_words:"true" required:"true""`
	StorageDriver string `split_words:"true" required:"true""`
	RedisUrl      string `split_words:"true"`
}

func NewConfig() (*Config, error) {
	var c Config
	err := envconfig.Process("", &c)
	if err != nil {
		return nil, err
	}
	return &c, nil
}
