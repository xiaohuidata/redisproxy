package server

import (
	"fmt"
	"redis_test/pkg/redisproxy"

	"github.com/spf13/viper"
)

func RunMutex() {
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

	mutext := conn.NewMutex("myMutex")
	err = mutext.Lock()
	if err != nil {
		fmt.Println("lock1 err", err)
		return
	}
	mutext.Unlock()
	fmt.Println("lock1", err)
	va, err := mutext.Valid()
	fmt.Println("Valid", va, err)
	fmt.Println("value", mutext.Value())
	va, err = mutext.Extend()
	fmt.Println("extend", va, err)
	fmt.Println("lock false", mutext.Lock())

	return
}
