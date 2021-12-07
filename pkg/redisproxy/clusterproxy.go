package redisproxy

import (
	"container/list"
	"errors"
	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
	"github.com/spf13/viper"
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
}

func (c *ClusterRedis) CreateConn() error {
	return c.createClusterRedis()
}

func (c *ClusterRedis) createClusterRedis() error {
	c.cluster = &redisc.Cluster{
		StartupNodes: viper.GetStringSlice("spec.redis.sentinels"),
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(300 * time.Millisecond)},
		CreatePool:   c.createPool,
	}
	if err := c.cluster.Refresh(); err != nil {
		return err
	}

	return nil
}

func (c *ClusterRedis) createPool(addr string, options ...redis.DialOption) (*redis.Pool, error) {
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
			if viper.GetString("spec.redis.passwd") != "" {
				if _, err := c.Do("AUTH", viper.GetString("spec.redis.passwd")); err != nil {
					c.Close()
					return nil, err
				}
			}
			/*
				if _, err := c.Do("SELECT", viper.GetString("spec.redis.dbindex")); err != nil {
					c.Close()
					return nil, err
				}
			*/
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

func (c *ClusterRedis) Get() (ClientProxy, redis.Conn, error) {
	proxy := new(ClusterProxy)
	proxy.conn = c.cluster.Get()
	retryConn, err := redisc.RetryConn(proxy.conn, 3, 100*time.Millisecond)
	proxy.retryConn = retryConn
	proxy.Type = CLUSTER
	return proxy, proxy.conn, err
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
