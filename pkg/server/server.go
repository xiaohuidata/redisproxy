package server

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
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

func ScriptAddOrZincrby(args ...interface{}) (interface{}, error) {
	if len(args) != 5 {
		return nil, errors.New("args len field")
	}

	conn := args[0].(redisproxy.ClientProxy)
	key1 := args[1]
	key2 := args[2]
	arg1 := args[3]
	arg2 := args[4]
	value, err := redis.Int64(conn.Do("EXISTS", key1))
	fmt.Println("EXISTS", value, err)

	var inter interface{}
	if value == 0 {
		inter, err = conn.Do("SET", key1, arg1)
	} else {
		inter, err = conn.Do("SET", key2, arg2)
	}
	fmt.Println("SET", inter, err)
	return inter, err
}

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
	v, err = conn.Receive()
	fmt.Println(v, err)
	v, err = conn.Receive()
	fmt.Println(v, err)
	v, err = conn.Receive()
	fmt.Println(v, err)

	mutext := conn.NewMutex("myMutex")
	err = mutext.Lock()
	if err != nil {
		fmt.Println("lock1 err", err)
		return
	}
	fmt.Println("lock1", err)
	defer mutext.Unlock()
	v, err = mutext.Valid()
	fmt.Println("Valid", v, err)
	fmt.Println("value", mutext.Value())
	v, err = mutext.Extend()
	fmt.Println("extend", v, err)
	fmt.Println(mutext.Lock())

	scriptfunc := ScriptAddOrZincrby
	luafunc := conn.NewScript(2, scriptfunc)
	hash, err = luafunc.Ints()
	if err != nil {
		fmt.Println("luafunc ints fail", err)
		return
	}
	fmt.Println("hash", hash)
	luafunc.SendHash(conn, "key1", "key2", "value1", "value2")
	err = conn.Flush()
	v, err = conn.Receive()
	fmt.Println(v, err)

	return
}
