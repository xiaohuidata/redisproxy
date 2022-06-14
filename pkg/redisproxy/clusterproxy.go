package redisproxy

import (
	"container/list"
	"errors"
	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
	"math/rand"
	"time"
)

type ReceiveType struct {
	val interface{}
	err error
}

type ClusterProxy struct {
	receives  list.List
	front     *list.Element
	conn      redis.Conn
	retryConn redis.Conn
	Type      RedisType
}

type ClusterRedis struct {
	cluster *redisc.Cluster
	passwd  string
	db      int
}

func (c *ClusterRedis) CreateConn(addrs []string, passwd string, name string, db int) error {
	return c.createClusterRedis(addrs, passwd, db)
}

func (c *ClusterRedis) createClusterRedis(addrs []string, passwd string, db int) error {
	c.cluster = &redisc.Cluster{
		StartupNodes: addrs,
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(300 * time.Millisecond)},
		CreatePool:   c.createPool,
	}
	c.passwd = passwd
	c.db = db
	if err := c.cluster.Refresh(); err != nil {
		return err
	}

	return nil
}

func (c *ClusterRedis) createPool(addr string, options ...redis.DialOption) (*redis.Pool, error) {
	passwd := c.passwd
	db := c.db
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   2000,
		Wait:        true,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr, options...)
			if err != nil {
				return nil, err
			}
			if passwd != "" {
				if _, err := c.Do("AUTH", passwd); err != nil {
					c.Close()
					return nil, err
				}
			}
			if db != 0 {
				if _, err := c.Do("SELECT", db); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}, nil
}

func (c *ClusterRedis) Stats() map[string]redis.PoolStats {
	return c.cluster.Stats()
}

func (c *ClusterRedis) Get() (ClientProxy, error) {
	proxy := new(ClusterProxy)
	proxy.conn = c.cluster.Get()
	retryConn, err := redisc.RetryConn(proxy.conn, 3, 100*time.Millisecond)
	proxy.retryConn = retryConn
	proxy.Type = CLUSTER
	return proxy, err
}

func (c *ClusterProxy) GetType() RedisType {
	return c.Type
}

func (c *ClusterProxy) NewScript(argc int, strong interface{}) ScriptInterface {
	script := new(ClusterScript)
	conn := new(ClientProxy)
	*conn = c
	script.NewScript(conn, argc, strong)
	return script
}

func (c *ClusterProxy) NewMutex(name string) *Mutex {
	return &Mutex{
		name:   name,
		expiry: 8 * time.Second,
		tries:  32,
		delayFunc: func(tries int) time.Duration {
			return time.Duration(rand.Intn(maxRetryDelayMilliSec-minRetryDelayMilliSec)+minRetryDelayMilliSec) * time.Millisecond
		},
		genValueFunc:  genValue,
		driftFactor:   0.01,
		timeoutFactor: 0.05,

		conn: func() *ClientProxy {
			conn := new(ClientProxy)
			*conn = c
			return conn
		}(),
	}
}

func (c *ClusterProxy) Do(cmd string, args ...interface{}) (interface{}, error) {
	return c.retryConn.Do(cmd, args...)
}

func (c *ClusterProxy) DoWithTimeout(timeout time.Duration, cmd string, args ...interface{}) (v interface{}, err error) {
	retryConn, err := redisc.RetryConn(c.conn, 3, 15*time.Second)
	if err != nil {
		return nil, err
	}
	return retryConn.Do(cmd, args...)
}

func (c *ClusterProxy) Send(cmd string, args ...interface{}) error {
	val, err := c.Do(cmd, args...)
	c.receives.PushBack(ReceiveType{val, err})
	return err
}

func (c *ClusterProxy) PushBack(receive ReceiveType) {
	c.receives.PushBack(receive)
}

func (c *ClusterProxy) Flush() error {
	c.front = c.receives.Front()
	return nil
}

func (c *ClusterProxy) Receive() (interface{}, error) {
	if c.front == nil {
		return nil, errors.New("receives, nil")
	}
	val, ok := c.front.Value.(ReceiveType)
	if !ok {
		return val.val, errors.New("ReceiveType false")
	}
	c.receives.Remove(c.front)
	c.front = c.receives.Front()
	return val.val, val.err
}

func (c *ClusterProxy) Close() error {
	var next *list.Element
	for e := c.receives.Front(); e != nil; e = next {
		next = e.Next()
		c.receives.Remove(e)
	}
	c.front = nil
	return c.conn.Close()
}

func (c *ClusterProxy) Err() error {
	return c.conn.Err()
}
