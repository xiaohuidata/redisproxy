package redisproxy

import (
	"github.com/gomodule/redigo/redis"
)

type ScriptProxy struct {
	strong interface{}
	conn   *ClientProxy
}

type ScriptInterface interface {
	NewScript(*ClientProxy, int, interface{})
	Ints() (string, error)
	SendHash(args ...interface{}) error
}

type ClusterScript struct {
	ScriptProxy
	lua func(argv ...interface{})
}

func (c *ClusterScript) NewScript(conn *ClientProxy, argc int, strong interface{}) {
	c.strong = strong
	c.conn = conn
}

func (c *ClusterScript) Ints() (string, error) {
	return "", nil
}

func (c *ClusterScript) SendHash(args ...interface{}) error {
	ScriptAddOrZincrby := c.strong.(func(...interface{}) (int, error))
	ScriptAddOrZincrby(args...)
	return nil
}

type SentinelScript struct {
	ScriptProxy
	lua *redis.Script
}

func (s *SentinelScript) NewScript(conn *ClientProxy, argc int, strong interface{}) {
	s.strong = redis.NewScript(argc, strong.(string))
	s.conn = conn
}

func (s *SentinelScript) Ints() (string, error) {
	s.lua = s.strong.(*redis.Script)
	hash := s.lua.Hash()
	val, err := redis.Ints((*s.conn).Do("SCRIPT", "EXISTS", hash))
	if val[0] == 0 {
		err = s.lua.Load(*s.conn)
	}
	return hash, err
}

func (s *SentinelScript) SendHash(args ...interface{}) error {
	args = args[1:]
	return s.lua.SendHash(*s.conn, args...)
}
