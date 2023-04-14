package server

import (
	"errors"
	"fmt"
	"redis_test/pkg/redisproxy"

	"github.com/gomodule/redigo/redis"
	"github.com/spf13/viper"
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

	SCRIPT_SADD = `
		return redis.call('SADD', 124, 124)
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

type State2Event{
	state string
	event string
}

const (
	state = map[State2Event]string{
		State2Event{
			"1",
			"2",
		}:"3",
	}
	arr = `state = {1 = {}}`

	SCRIPT = `
	
	local state = redis.call('HMGET', KEYS[1], ARGV[1])
	if state == 
	`
)

func RunLua() {
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

	script := SCRIPT_SADD
	lua := conn.NewScript(1, script)
	hash, err := lua.Ints()
	fmt.Println("lua init", hash, err)
	lua.SendHash(conn, "reconnoitre-20200910-010-Telecom-010-global-1010-1470-all")
	conn.Flush()
	v, err := conn.Receive()
	fmt.Println("lua ", v, err)

	script = SCRIPT_ZADD_OR_ZINCRBY
	lua = conn.NewScript(1, script)
	hash, err = lua.Ints()
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
	v, err = redis.Int(conn.Receive())
	fmt.Println(v, err)
	v, err = redis.Int(conn.Receive())
	fmt.Println(v, err)
	v, err = redis.Int(conn.Receive())
	fmt.Println(v, err)
	v, err = redis.Int(conn.Receive())
	fmt.Println(v, err)
	v, err = redis.Int(conn.Receive())
	fmt.Println(v, err)
	v, err = redis.Int(conn.Receive())
	fmt.Println(v, err)

	if conn.GetType() == redisproxy.CLUSTER {
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
		vm, err := conn.Receive()
		fmt.Println(vm, err)
	}

	return
}
