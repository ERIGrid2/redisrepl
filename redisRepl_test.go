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
	redis "github.com/garyburd/redigo/redis"
	"testing"
)

// Test 1
func TestHashCmp1(t *testing.T) {

}

func redisConn() redis.Conn {
	var redisAddr = ":6379"
	// create a Redis connection pool
	pool := redis.Pool{
		MaxIdle: 3,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisAddr)
		},
	}
	return pool.Get()
}
