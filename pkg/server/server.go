package server

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"redis_test/pkg/redisproxy"
)

const (
	SCRIPT_ZADD_OR_ZINCRBY = `
	local member = redis.call('ZRANK', KEYS[1], ARGV[2])
	if member then
		return redis.call('ZINCRBY', KEYS[1], 1, ARGV[2])
	else
		return redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
	end
  `
)

func Run() {
	fmt.Println("redis run")
	redisCli := new(redisproxy.SentinelRedis)
	err := redisCli.CreateConn()
	if err != nil {
		fmt.Println("crete fail", err)
		return
	}

	conn, oconn, err := redisCli.Get()
	defer conn.Close()
	if err != nil {
		fmt.Println("conn fail", err)
		return
	}
	script := SCRIPT_ZADD_OR_ZINCRBY
	lua := conn.NewScript(1, script)
	hash, err := lua.Ints()
	if err != nil {
		fmt.Println("lua ints fail", err)
		return
	}
	fmt.Println("hash", hash)
	lua.SendHash(conn, "mykey", 111111, 111111)

	olua := redis.NewScript(1, SCRIPT_ZADD_OR_ZINCRBY)
	redis.Ints(oconn.Do("SCRIPT", "EXISTS", olua.Hash()))
	/*	if val2[0] == 0 {
		err = olua.Load(oconn)
	}*/
	olua.SendHash(oconn, "key111", 111, 111)

	err = conn.Flush()
	fmt.Println(err)
	return
}
