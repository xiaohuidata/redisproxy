package server

import (
	"fmt"
	"github.com/spf13/viper"
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
	fmt.Println(viper.GetStringSlice("spec.redis.type"), viper.GetStringSlice("spec.redis.sentinels"))
	redisCli := func() redisproxy.ClientRedis {
		if viper.GetString("spec.redis.type") == "Sentinel" {
			return new(redisproxy.SentinelRedis)
		} else if viper.GetString("spec.redis.type") == "Cluster" {
			return new(redisproxy.ClusterRedis)
		}
		return nil
	}()
	if redisCli == nil {
		fmt.Println("new redis failed.")
		return
	}
	err := redisCli.CreateConn(viper.GetStringSlice("spec.redis.sentinels"), viper.GetString("spec.redis.passwd"), viper.GetString("spec.redis.masterName"), viper.GetInt("spec.redis.db"))
	if err != nil {
		fmt.Println("crete fail", err)
		return
	}

	conn, err := redisCli.Get()
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

	lua.SendHash(conn, "key111", 111, 111)
	lua.SendHash(conn, "my123456", 2222, 2222)
	lua.SendHash(conn, "my156", 2222, 2222)
	lua.SendHash(conn, "23456", 2222, 2222)
	lua.SendHash(conn, "ispjfk", 2222, 2222)

	err = conn.Flush()
	v, err := conn.Receive()
	fmt.Println(v, err)
	v, err = conn.Receive()
	fmt.Println(v, err)
	v, err = conn.Receive()
	fmt.Println(v, err)
	return
}
