package redisproxy

import (
	"errors"
	"fmt"
	"time"

	"github.com/FZambia/sentinel"
	"github.com/gomodule/redigo/redis"
	"github.com/spf13/viper"
)

type SentinelProxy struct {
	conn redis.Conn
	Type RedisType
}

type SentinelRedis struct {
	sentinel *sentinel.Sentinel
	p        *redis.Pool
}

func (s *SentinelRedis) CreateConn() error {
	return s.createSentinelRedis()
}

func (s *SentinelRedis) createSentinelRedis() error {
	fmt.Println("spec.redis.sentinels", viper.GetStringSlice("spec.redis.sentinels"))
	fmt.Println("spec.redis.masterName", viper.GetString("spec.redis.masterName"))
	s.sentinel = &sentinel.Sentinel{
		Addrs:      viper.GetStringSlice("spec.redis.sentinels"),
		MasterName: viper.GetString("spec.redis.masterName"),
		Dial: func(addr string) (redis.Conn, error) {
			timeout := 300 * time.Millisecond
			c, err := redis.Dial("tcp", addr,
				redis.DialPassword(viper.GetString("spec.redis.passwd")),
				redis.DialConnectTimeout(timeout),
				redis.DialReadTimeout(timeout),
				redis.DialWriteTimeout(timeout),
			)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
	err := s.sentinel.Discover()

	if err != nil {
		return err
	}

	s.p = &redis.Pool{
		MaxIdle:      100,
		MaxActive:    2000,
		Wait:         true,
		IdleTimeout:  180 * time.Second,
		TestOnBorrow: redisTestOnBorrow,
		Dial: func() (redis.Conn, error) {
			masterAddr, e := s.sentinel.MasterAddr()

			if e != nil {
				return nil, e
			}
			timeout := 300 * time.Millisecond
			conn, e := redis.Dial("tcp", masterAddr,
				redis.DialConnectTimeout(timeout),
				redis.DialReadTimeout(timeout),
				redis.DialWriteTimeout(timeout))

			if e != nil {
				return nil, e
			}
			return conn, nil
		},
	}

	conn := s.p.Get()
	defer conn.Close()

	_, err = conn.Do("PING")

	if err != nil {
	}
	return err
}

func redisTestOnBorrow(conn redis.Conn, t time.Time) error {
	if sentinel.TestRole(conn, "master") {
		if time.Since(t) < time.Minute {
			return nil
		}
		_, err := conn.Do("PING")
		return err
	}
	return errors.New("the server of this connection is not a master")
}

func (s *SentinelRedis) Stats() map[string]redis.PoolStats {
	stat := s.p.Stats()
	stats := make(map[string]redis.PoolStats, 1)
	stats["Sentinel"] = stat
	return stats
}

func (s *SentinelRedis) Get() (ClientProxy, redis.Conn, error) {
	proxy := new(SentinelProxy)
	proxy.conn = s.p.Get()
	proxy.Type = SENTINEL
	return proxy, proxy.conn, nil
}

func (s *SentinelProxy) GetType() RedisType {
	return s.Type
}

func (s *SentinelProxy) NewScript(argc int, strong interface{}) ScriptInterface {
	script := new(SentinelScript)
	conn := new(ClientProxy)
	*conn = s
	fmt.Println("NewScript 1", conn, *conn, s, *s)
	script.NewScript(conn, argc, strong)
	return script
}

func (s *SentinelProxy) Do(cmd string, args ...interface{}) (interface{}, error) {
	return s.conn.Do(cmd, args...)
}

func (s *SentinelProxy) DoWithTimeout(timeout time.Duration, cmd string, args ...interface{}) (v interface{}, err error) {
	connT := s.conn.(redis.ConnWithTimeout)
	return connT.DoWithTimeout(timeout, cmd, args...)
}

func (s *SentinelProxy) Send(cmd string, args ...interface{}) error {
	fmt.Println("Conn Send", cmd, "|", args)
	return s.conn.Send(cmd, args...)
}

func (s *SentinelProxy) Flush() error {
	return s.conn.Flush()
}

func (s *SentinelProxy) Receive() (interface{}, error) {
	return s.conn.Receive()
}

func (s *SentinelProxy) Close() error {
	return s.conn.Close()
}

func (s *SentinelProxy) Err() error {
	return s.conn.Err()
}
