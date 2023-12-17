package server

import (
	"fmt"
	"redis_test/pkg/redisproxy"

	"github.com/gomodule/redigo/redis"
	"github.com/spf13/viper"
)

func RunSlave() {
	fmt.Println("redis slave run")
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
	sconn, err := redisCli.GetSlave()
	defer sconn.Close()
	if err != nil {
		fmt.Println("slave conn fail", err)
		return
	}

	conn.Do("SET", "1", "1")
	conn.Do("SET", "2", "2")
	conn.Do("SET", "3", "3")
	conn.Do("SET", "4", "4")
	conn.Do("SET", "5", "5")
	conn.Do("SET", "6", "6")
	conn.Do("SET", "7", "7")

	value, err := redis.String(sconn.Do("GET", "1"))
	fmt.Println("Get 1", value, err)
	value, err = redis.String(sconn.Do("GET", "2"))
	fmt.Println("Get 2", value, err)
	value, err = redis.String(sconn.Do("GET", "3"))
	fmt.Println("Get 3", value, err)
	value, err = redis.String(sconn.Do("GET", "4"))
	fmt.Println("Get 4", value, err)
	value, err = redis.String(sconn.Do("GET", "5"))
	fmt.Println("Get 5", value, err)
	value, err = redis.String(sconn.Do("GET", "6"))
	fmt.Println("Get 6", value, err)
	value, err = redis.String(sconn.Do("GET", "7"))
	fmt.Println("Get 7", value, err)

	// 集群模式可以正常执行
	sconn.Do("SET", "8", "8")
	value, err = redis.String(sconn.Do("GET", "8"))
	fmt.Println("Get 8", value, err)

	return
}
