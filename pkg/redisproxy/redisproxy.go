package redisproxy

import (
	"github.com/gomodule/redigo/redis"
	"time"
)

type RedisType int

const (
	SENTINEL RedisType = iota
	CLUSTER
)

type ClientRedis interface {
	CreateConn() error
	Get() (ClientProxy, redis.Conn, error)
	Stats() map[string]redis.PoolStats
}

type ClientProxy interface {
	GetType() RedisType
	NewScript(argc int, strong interface{}) ScriptInterface
	Do(cmd string, args ...interface{}) (interface{}, error)
	DoWithTimeout(timeout time.Duration, cmd string, args ...interface{}) (v interface{}, err error)
	Send(cmd string, args ...interface{}) error
	Flush() error
	Receive() (interface{}, error)
	Close() error
	Err() error
}
