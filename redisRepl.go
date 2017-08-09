/*
 Main redisRepl program.

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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	redis "github.com/garyburd/redigo/redis"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
	"os"
)

const version = "master"
var remKey, remVal string
var remHashKey string
var remHashVal map[string]interface{}
// version flag
var showVersion = flag.Bool("version", false, "Show version and exit")
// redis flags
var redisAddr = flag.String("raddr", ":6379", "Redis server address")
var nam = flag.String("nam", "dummyRI", "RI namespace")
var remCmds = flag.Bool("rc", false, "Enable remote commands (disabled by default)")
// https flags
var httpsAddr = flag.String("haddr", "https://localhost/redis", "HTTPS server address")
var certFile = flag.String("cert", "someCertFile", "A PEM encoded certificate file.")
var keyFile = flag.String("key", "someKeyFile", "A PEM encoded private key file.")
var caFile = flag.String("CA", "someCertCAFile", "A PEM encoded CA's certificate file.")

// Main functions:
// 1. connect to local Redis via TCP and remote redis via HTTPS
// 2. replicate every local 'set' command whose key begins with
//    <nam> remotely via HTTPS
// 3. replicate everly local 'hset' or 'hmset' command whose key
//    begins with <nam> remotely via HTTPS
// 4. optionally, replicate any remote 'set' command whose key
//    begins with <nam> into the local Redis.
func main() {
	flag.Parse()
	if *showVersion {
		log.Printf("RedisRepl version: %s\n", version)
		os.Exit(0)
	}
	// create a Redis connection pool
	pool := redis.Pool{
		MaxIdle: 3,
		Dial: func () (redis.Conn, error) {
			return redis.Dial("tcp", *redisAddr)
		},
	}
	// connection to Redis, for local listener
	cloc := pool.Get()
	defer cloc.Close()
	// additional connection to Redis for pubsub
	csub := pool.Get()
	defer csub.Close()
	client := https_client()
	// Enable key-event notifications for strings and hashes
	cloc.Do("CONFIG", "set", "notify-keyspace-events", "K$h")
	setupHttps(pool, client)
	// subscribe to events from local Redis instance
	log.Printf("Replicating local Redis instance to remote address: %s\n", *httpsAddr)
	psc := redis.PubSubConn{Conn: csub}
	psc.PSubscribe("__keyspace@0__:*")
	for {
		switch v := psc.Receive().(type) {
		case redis.PMessage:
			cmd := string(v.Data)
			key := strings.TrimPrefix(v.Channel, "__keyspace@0__:")
			if strings.HasPrefix(key, *nam) {
				handleCmd(cmd, key, cloc, client)
			}
		case redis.Subscription:
		case error:
			log.Println("Error in local Redis subscription")
			return
		}
	}
}

func setupHttps(pool redis.Pool, client *http.Client) {
	// if needed, start listening for remote events
	if *remCmds == true {
		// another connection to Redis, for remote receiver
		crem := pool.Get()
		go receive(client, crem)
	}
}

func handleCmd(cmd, key string, c redis.Conn, client *http.Client) {
	switch cmd {
	case "set":	
		val, e := redis.String(c.Do("GET", key))
		if e != nil {
			log.Printf("Error reading %s from Redis\n", key)
		}
		if key != remKey || val != remVal {
			redisData := "SET/" + escape(key) + "/" + escape(val)
			log.Printf("(local)->(remote) %s %s\n", key, val)
			send(*httpsAddr, redisData, client)
		}
	case "hset":
		fields, e := redis.StringMap(c.Do("HGETALL", key))
		if e != nil {
			log.Printf("Error reading %s from Redis\n", key)
		}
		if key != remHashKey || hashCmp(c, key, remHashVal) == false {
			redisData := "HMSET/" + key
			for k, v := range fields {
				redisData = redisData + "/" + escape(k)
				redisData = redisData + "/" + escape(v)
			}
			log.Printf("(local)->(remote) hash %s\n", key)
			send(*httpsAddr, redisData, client)
		}
	default:
		log.Printf("(local) Unhandled event: %s\n", cmd)
	}
}

// Create a new HTTPS client with the given certificate files.
func https_client() *http.Client {
	// Load client cert
	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Fatal(err)
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(*caFile)
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}
	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,

	}
	client := &http.Client{
		Transport: transport,
	}
	return client
}

// Send a new command to remote Redis via HTTPS. The command uses the Webdis
// format. 
func send(addr, cmd string, c *http.Client) (string, interface{}) {
	// send the actual command
	resp, e := c.Post(addr+"/", "text/plain", strings.NewReader(cmd))
	if e != nil {
		log.Println(e)
		return "", e
	}
	defer resp.Body.Close()
	// return response parsed from JSON
	k, v := parseSingle(json.NewDecoder(resp.Body))
	ioutil.ReadAll(resp.Body)
	return k, v;
}

// Parse a JSON message, assuming it has a single field (like a reply after
// command execution). Each command returns a JSON object with the command
// as a key and the result as a value.
func parseSingle(dec *json.Decoder) (string, interface{}) {
	var rawMsg map[string]interface{}
	e := dec.Decode(&rawMsg)
	if e != nil {
		log.Println(e)
		return "", e
	}
	for k, v := range rawMsg {
		return k, v
	}
	return "", nil
}

// Receive and handle a message from the remote subscription.
func receive(client *http.Client, c redis.Conn) {
	log.Printf("Subscribing for remote changes to the namespace: %s\n", *nam)
	// Enable key-event notifications for strings and hashes
	send(*httpsAddr, "CONFIG/set/notify-keyspace-events/K$h", client)

	resp, e := client.Get(*httpsAddr + "/PSUBSCRIBE/__keyspace@0__:" + *nam + "*")
	if e != nil {
		log.Println(e)
		time.Sleep(10 * time.Second)
		go receive(client, c)
		return
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var rawMsg interface{}
		err := dec.Decode(&rawMsg)
		if err != nil {
			log.Fatal(err)
		}
		m := rawMsg.(map[string]interface{})
		for k, v := range m {
			switch k {
			case "PSUBSCRIBE":
				sub(client, v, c)
			}
		}
	}
	// TODO: retry connection
	log.Println("Error: remote HTTPS connection down, retrying in 10 seconds")
	time.Sleep(10 * time.Second)
	go receive(client, c)
}

func sub(client *http.Client, v interface{}, c redis.Conn) {
	redisPayload := v.([]interface{})
	msgType := redisPayload[0].(string)
	if msgType == "pmessage" {
		keySpace := redisPayload[2].(string)
		cmd := redisPayload[3].(string)
		key := strings.TrimPrefix(keySpace, "__keyspace@0__:")
		switch cmd {
		case "set":
			_, res := send(*httpsAddr, "GET/" + escape(key), client)
			val := res.(string)
			actVal, e := redis.String(c.Do("GET", key))
			if e != nil {
				log.Printf("Reading %s from Redis: %s\n", key, e)
			}
			// write the value only if it is different from the one
			// already stored.
			if actVal != val {
				log.Printf("(remote)->(local) %s %s\n", key, val)
				set(key, val, c)
			}
		case "hset":
			_, res := send(*httpsAddr, "HGETALL/" + escape(key), client)
			val := res.(map[string]interface{})
			// write the value only if it is different from the one
			// already stored. 
			if hashCmp(c, key, val) == false {
				log.Printf("(remote)->(local) %s %v\n", key, val)
				hset(key, val, c)
			}
		}
	}
}

func set(key string, val string, c redis.Conn) {
	c.Do("SET", key, val)
	remKey = key
	remVal = val
}

func hset(key string, val map[string]interface{}, c redis.Conn) {
	args := []interface{}{key}
	for hk, hv := range val {
		args = append(args, hk, hv)
	}
	_, e := c.Do("HMSET", args...)
	if e != nil {
		log.Println(e)
	}
	remHashKey = key
	remHashVal = val
}

func escape(msg string) string {
	escaped := strings.Replace(msg, "/", "%2f", -1)
	escaped = strings.Replace(escaped, ".", "%2e", -1)
	return escaped
}

func hashCmp(c redis.Conn, key string, hash map[string]interface{}) bool {
	if hash == nil {
		return false
	}
	if len(hash) == 0 {
		return false
	}
	loc, e := redis.StringMap(c.Do("HGETALL", key))
	if e != nil {
		log.Fatal(e)
	}
	if len(loc) != len(hash) {
		return false
	}
	for lk, lv := range loc {
		hv := hash[lk]
		if hv != lv {
			return false
		}
	}
	return true
}
