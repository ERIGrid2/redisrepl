package main

import (
	"testing"
	redis "github.com/garyburd/redigo/redis"
)

// Test 1
// test hash comparison.
// No error should occur.
func TestHashCmp1(t *testing.T) {
	c := redisConn()
	hashName := "test1"
	hash := map[string]interface{} {
		"one": "1",
		"two": "2",
	}
	for hk, hv := range hash {
		c.Do("HSET", hashName, hk, hv)
	}
	res := hashCmp(c, hashName, hash)
	if res == false {
		t.FailNow()
	}
}

// Test 2
// test hash comparison.
// It should fail.
func TestHashCmp2(t *testing.T) {
	c := redisConn()
	hashName := "test1"
	hash := map[string]interface{} {
		"one": "1",
		"two": "2",
	}
	for hk, hv := range hash {
		c.Do("HSET", hashName, hk, hv)
	}
	hash["three"] = "3";
	res := hashCmp(c, hashName, hash)
	if res == true {
		t.FailNow()
	}
}

func redisConn() redis.Conn {
	var redisAddr = ":6379"
	// create a Redis connection pool
	pool := redis.Pool{
		MaxIdle: 3,
		Dial: func () (redis.Conn, error) {
			return redis.Dial("tcp", redisAddr)
		},
	}
	return pool.Get()
}
