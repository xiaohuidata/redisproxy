package redisproxy

import (
	"errors"
	"math/rand"
	"time"

	"github.com/FZambia/sentinel"
	"github.com/gomodule/redigo/redis"
)

type SentinelProxy struct {
	conn redis.Conn
	Type RedisType
}

type SentinelRedis struct {
	sentinel *sentinel.Sentinel
	p        *redis.Pool
	sp       *redis.Pool
}

func (s *SentinelRedis) CreateConn(addrs []string, passwd string, name string, db int) error {
	return s.createSentinelRedis(addrs, passwd, name, db)
}

func (s *SentinelRedis) createSentinelRedis(addrs []string, passwd string, name string, db int) error {
	s.sentinel = &sentinel.Sentinel{
		Addrs:      addrs,
		MasterName: name,
		Dial: func(addr string) (redis.Conn, error) {
			timeout := 300 * time.Millisecond
			c, err := redis.Dial("tcp", addr,
				redis.DialConnectTimeout(timeout),
				redis.DialReadTimeout(timeout),
				redis.DialWriteTimeout(timeout))
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
			conn, e := func() (redis.Conn, error) {
				if db != 0 {
					return redis.Dial("tcp", masterAddr,
						redis.DialPassword(passwd),
						redis.DialDatabase(db),
						redis.DialConnectTimeout(timeout),
						redis.DialReadTimeout(timeout),
						redis.DialWriteTimeout(timeout))
				} else {
					return redis.Dial("tcp", masterAddr,
						redis.DialPassword(passwd),
						redis.DialConnectTimeout(timeout),
						redis.DialReadTimeout(timeout),
						redis.DialWriteTimeout(timeout))
				}
			}()

			if e != nil {
				return nil, e
			}
			return conn, nil
		},
	}

	s.sp = &redis.Pool{
		MaxIdle:      100,
		MaxActive:    2000,
		Wait:         true,
		IdleTimeout:  180 * time.Second,
		TestOnBorrow: redisTestOnBorrow,
		Dial: func() (redis.Conn, error) {
			slaveAddr, e := s.sentinel.SlaveAddrs()

			if e != nil {
				return nil, e
			}
			slavelen := len(slaveAddr)
			if slavelen == 0 {
				return nil, errors.New("slave null")
			}
			timeout := 300 * time.Millisecond
			conn, e := func() (redis.Conn, error) {
				if db != 0 {
					return redis.Dial("tcp", slaveAddr[0],
						redis.DialPassword(passwd),
						redis.DialDatabase(db),
						redis.DialConnectTimeout(timeout),
						redis.DialReadTimeout(timeout),
						redis.DialWriteTimeout(timeout))
				} else {
					return redis.Dial("tcp", slaveAddr[0],
						redis.DialPassword(passwd),
						redis.DialConnectTimeout(timeout),
						redis.DialReadTimeout(timeout),
						redis.DialWriteTimeout(timeout))
				}
			}()

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
		return err
	}

	sconn := s.sp.Get()
	defer sconn.Close()
	_, err = sconn.Do("PING")

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

func (s *SentinelRedis) Get() (ClientProxy, error) {
	proxy := new(SentinelProxy)
	proxy.conn = s.p.Get()
	proxy.Type = SENTINEL
	return proxy, nil
}

func (s *SentinelRedis) GetSlave() (ClientProxy, error) {
	proxy := new(SentinelProxy)
	proxy.conn = s.sp.Get()
	proxy.Type = SENTINEL
	return proxy, nil
}

func (s *SentinelProxy) GetType() RedisType {
	return s.Type
}

func (s *SentinelProxy) NewScript(argc int, strong interface{}) ScriptInterface {
	script := new(SentinelScript)
	conn := new(ClientProxy)
	*conn = s
	script.NewScript(conn, argc, strong)
	return script
}

func (s *SentinelProxy) NewMutex(name string) *Mutex {
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
			*conn = s
			return conn
		}(),
	}
}

func (s *SentinelProxy) Do(cmd string, args ...interface{}) (interface{}, error) {
	return s.conn.Do(cmd, args...)
}

func (s *SentinelProxy) DoWithTimeout(timeout time.Duration, cmd string, args ...interface{}) (v interface{}, err error) {
	connT := s.conn.(redis.ConnWithTimeout)
	return connT.DoWithTimeout(timeout, cmd, args...)
}

func (s *SentinelProxy) Send(cmd string, args ...interface{}) error {
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

func (s *SentinelProxy) PushBack(receive ReceiveType) {
}
