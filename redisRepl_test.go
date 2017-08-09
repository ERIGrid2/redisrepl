/*
 redisRepl tests.

 Copyright 2017 Daniele Pala <daniele.pala@rse-web.it>                                                                                                                                                             

 This file is part of redisRepl. 

 redisRepl is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.                                                                                                                                                                               

 redisRepl is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of 
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with redisRepl. If not, see <http://www.gnu.org/licenses/>.
*/

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
