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
	CreateConn(addrs []string, passwd string, name string, db int) error
	Get() (ClientProxy, error)
	Stats() map[string]redis.PoolStats
}

type ClientProxy interface {
	GetType() RedisType
	NewScript(argc int, strong interface{}) ScriptInterface
	NewMutex(name string) *Mutex
	Do(cmd string, args ...interface{}) (interface{}, error)
	DoWithTimeout(timeout time.Duration, cmd string, args ...interface{}) (v interface{}, err error)
	Send(cmd string, args ...interface{}) error
	Flush() error
	Receive() (interface{}, error)
	Close() error
	Err() error
	PushBack(receive ReceiveType)
}
