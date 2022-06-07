package redisproxy

import (
	"github.com/gomodule/redigo/redis"
	"strings"
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
	lua  func(argv ...interface{})
	argc int
}

func (c *ClusterScript) NewScript(conn *ClientProxy, argc int, strong interface{}) {
	c.argc = argc
	c.strong = strong
	c.conn = conn
}

func (c *ClusterScript) Ints() (string, error) {
	return "", nil
}

func (c *ClusterScript) SendHash(args ...interface{}) error {
	if c.argc > 1 {
		c.sendKeysHash(args...)
	} else if c.argc < 0 {
		return nil
	} else {
		return c.sendKeyHash(args...)
	}
	return nil
}

// 多key调用，在写script使用redis函数，需要增加锁
func (c *ClusterScript) sendKeysHash(args ...interface{}) error {
	ScriptAddOrZincrby := c.strong.(func(...interface{}) (int, error))
	v, err := ScriptAddOrZincrby(args...)
	(*c.conn).PushBack(ReceiveType{v, err})
	return nil
}

func (c *ClusterScript) sendKeyHash(args ...interface{}) error {
	script := redis.NewScript(c.argc, c.strong.(string))
	argv := args[1:]
	argvs := append([]interface{}{script.Hash(), c.argc}, argv...)
	argve := append([]interface{}{c.strong.(string), c.argc}, argv...)
	v, err := (*c.conn).Do("EVALSHA", argvs...)
	if e, ok := err.(redis.Error); ok && strings.HasPrefix(string(e), "NOSCRIPT ") {
		v, err = (*c.conn).Do("EVAL", argve...)
	}
	(*c.conn).PushBack(ReceiveType{v, err})
	return err
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
