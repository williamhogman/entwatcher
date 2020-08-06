package main

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"go.uber.org/fx"
)

// Storage is an interface common for storage engines
type Storage interface {
	Close() error
	AddWatches(context.Context, string, []string, []string) error
	Match(context.Context, []string, []string) ([]string, error)
}

// RedisStorage is storage engine storing things to Redis
type RedisStorage struct {
	client *redis.Client
}

func (r *RedisStorage) Close() error {
	return r.client.Close()
}

// newRedisStorage creates a new redis storage
func newRedisStorage(c *Config) (Storage, error) {
	opts, err := redis.ParseURL(c.RedisUrl)
	if err != nil {
		return nil, err
	}
	return Storage(&RedisStorage{
		client: redis.NewClient(opts),
	}), nil
}

var ErrNoSuchStorageDriver = errors.New("No such storage driver found")

func NewStorage(lc fx.Lifecycle, c *Config) (Storage, error) {
	if c.StorageDriver == "redis" {
		res, err := newRedisStorage(c)
		if err != nil {
			return nil, err
		}
		lc.Append(fx.Hook{
			OnStop: func(ctx context.Context) error {
				return res.Close()
			},
		})
		return res, nil
	} else {
		return nil, ErrNoSuchStorageDriver
	}
}

func absoluteKey(path string) string {
	return "entwatcher_absolute_routes:" + path
}

func wildcardKey(path string) string {
	return "entwatcher_wildcard_routes:" + path
}

func constructKeys(absoluteKeys []string, wildcards []string) []string {
	keys := make([]string, 0, len(absoluteKeys)+len(wildcards))
	for _, x := range absoluteKeys {
		keys = append(keys, absoluteKey(x))
	}

	for _, x := range wildcards {
		keys = append(keys, wildcardKey(x))
	}
	return keys
}

func (rs *RedisStorage) AddWatches(ctx context.Context, value string, absoluteKeys []string, wildcards []string) error {
	keys := constructKeys(absoluteKeys, wildcards)
	_, err := rs.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, x := range keys {
			pipe.SAdd(ctx, x, value)
		}
		return nil
	})
	return err
}

func (rs *RedisStorage) Match(ctx context.Context, absoluteKeys []string, wildcards []string) ([]string, error) {
	keys := constructKeys(absoluteKeys, wildcards)
	return rs.client.SUnion(ctx, keys...).Result()
}
