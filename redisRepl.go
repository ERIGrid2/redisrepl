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
	"os"
	"strings"
	"time"
)

const version = "master"

// Replicator is a remote Redis replicator. It replicates every 'set' or 'hset'
// command from a local Redis instance to a remote one, using the Webdis
// protocol. Optionally, it can also work in the reverse direction.
type Replicator struct {
	httpsAddr        string                 // HTTPS server address
	client           *http.Client           // HTTPS client
	cloc, csub, crem redis.Conn             // Redis connections
	remKey, remVal   string                 // last remote key-value
	remHashKey       string                 // last remote hash key
	remHashVal       map[string]interface{} // last remote hash value
}

// ReplOpt contains options to be used by a Replicator.
type ReplOpt struct {
	redisAddr string       // Local Redis server address
	httpsAddr string       // HTTPS server address
	client    *http.Client // HTTPS client
	remCmds   bool         // enable remote -> local replication?
	nam       string       // namespace to replicate
}

func main() {
	// parse cmdline flags
	// version flag
	var showVersion = flag.Bool("version", false, "Show version and exit")
	// security certificates
	var certFile = flag.String("cert", "someCertFile", "A PEM encoded certificate file.")
	var keyFile = flag.String("key", "someKeyFile", "A PEM encoded private key file.")
	var caFile = flag.String("CA", "someCertCAFile", "A PEM encoded CA's certificate file.")
	// redis flags
	var redisAddr = flag.String("raddr", ":6379", "Redis server address")
	var remCmds = flag.Bool("rc", false, "Enable remote commands (disabled by default)")
	var nam = flag.String("nam", "dummyRI", "RI namespace")
	// https flags
	var httpsAddr = flag.String("haddr", "https://localhost/redis", "HTTPS server address")
	flag.Parse()
	// build replicator options
	client := https_client(*certFile, *keyFile, *caFile)
	if *showVersion {
		log.Printf("RedisRepl version: %s\n", version)
		os.Exit(0)
	}
	opts := ReplOpt{
		redisAddr: *redisAddr,
		httpsAddr: *httpsAddr,
		client:    client,
		remCmds:   *remCmds,
		nam:       *nam,
	}
	// start replicator
	replicator := Replicator{}
	replicator.Start(opts)
}

// Main functions:
// 1. connect to local Redis via TCP and remote redis via HTTPS
// 2. replicate every local 'set' command whose key begins with
//    <nam> remotely via HTTPS
// 3. replicate everly local 'hset' or 'hmset' command whose key
//    begins with <nam> remotely via HTTPS
// 4. optionally, replicate any remote 'set' command whose key
//    begins with <nam> into the local Redis.
func (r *Replicator) Start(opts ReplOpt) {
	r.httpsAddr = opts.httpsAddr
	r.client = opts.client
	// create a Redis connection pool
	pool := redis.Pool{
		MaxIdle: 3,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", opts.redisAddr)
		},
	}
	// connection to Redis, for local listener
	r.cloc = pool.Get()
	defer r.cloc.Close()
	// additional connection to Redis for pubsub
	r.csub = pool.Get()
	defer r.csub.Close()
	// Enable key-event notifications for strings and hashes
	r.cloc.Do("CONFIG", "set", "notify-keyspace-events", "K$h")
	// if needed, start listening for remote events
	if opts.remCmds == true {
		// another connection to Redis, for remote receiver
		r.crem = pool.Get()
		go r.receive(opts.nam)
	}
	// subscribe to events from local Redis instance
	log.Printf("Replicating local Redis instance to remote address: %s\n", r.httpsAddr)
	psc := redis.PubSubConn{Conn: r.csub}
	psc.PSubscribe("__keyspace@0__:*")
	for {
		switch v := psc.Receive().(type) {
		case redis.PMessage:
			cmd := string(v.Data)
			key := strings.TrimPrefix(v.Channel, "__keyspace@0__:")
			if strings.HasPrefix(key, opts.nam) {
				r.handleCmd(cmd, key)
			}
		case redis.Subscription:
		case error:
			log.Println("Error in local Redis subscription")
			return
		}
	}
}

func (r *Replicator) handleCmd(cmd, key string) {
	switch cmd {
	case "set":
		val, e := redis.String(r.cloc.Do("GET", key))
		if e != nil {
			log.Printf("Error reading %s from Redis\n", key)
		}
		if key != r.remKey || val != r.remVal {
			redisData := "SET/" + escape(key) + "/" + escape(val)
			log.Printf("(local)->(remote) %s %s\n", key, val)
			send(r.httpsAddr, redisData, r.client)
		}
	case "hset":
		fields, e := redis.StringMap(r.cloc.Do("HGETALL", key))
		if e != nil {
			log.Printf("Error reading %s from Redis\n", key)
		}
		if key != r.remHashKey || hashCmp(r.cloc, key, r.remHashVal) == false {
			redisData := "HMSET/" + key
			for k, v := range fields {
				redisData = redisData + "/" + escape(k)
				redisData = redisData + "/" + escape(v)
			}
			log.Printf("(local)->(remote) hash %s\n", key)
			send(r.httpsAddr, redisData, r.client)
		}
	default:
		log.Printf("(local) Unhandled event: %s\n", cmd)
	}
}

// Create a new HTTPS client with the given certificate files.
func https_client(certFile, keyFile, caFile string) *http.Client {
	// Load client cert
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatal(err)
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caFile)
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
	return k, v
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
func (r *Replicator) receive(nam string) {
	log.Printf("Subscribing for remote changes to the namespace: %s\n", nam)
	// Enable key-event notifications for strings and hashes
	send(r.httpsAddr, "CONFIG/set/notify-keyspace-events/K$h", r.client)

	resp, e := r.client.Get(r.httpsAddr + "/PSUBSCRIBE/__keyspace@0__:" + nam + "*")
	if e != nil {
		log.Println(e)
		time.Sleep(10 * time.Second)
		go r.receive(nam)
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
				r.sub(v)
			}
		}
	}
	// TODO: retry connection
	log.Println("Error: remote HTTPS connection down, retrying in 10 seconds")
	time.Sleep(10 * time.Second)
	go r.receive(nam)
}

func (r *Replicator) sub(v interface{}) {
	redisPayload := v.([]interface{})
	msgType := redisPayload[0].(string)
	if msgType == "pmessage" {
		keySpace := redisPayload[2].(string)
		cmd := redisPayload[3].(string)
		key := strings.TrimPrefix(keySpace, "__keyspace@0__:")
		switch cmd {
		case "set":
			_, res := send(r.httpsAddr, "GET/"+escape(key), r.client)
			val := res.(string)
			actVal, e := redis.String(r.crem.Do("GET", key))
			if e != nil {
				log.Printf("Reading %s from Redis: %s\n", key, e)
			}
			// write the value only if it is different from the one
			// already stored.
			if actVal != val {
				log.Printf("(remote)->(local) %s %s\n", key, val)
				r.set(key, val)
			}
		case "hset":
			_, res := send(r.httpsAddr, "HGETALL/"+escape(key), r.client)
			val := res.(map[string]interface{})
			// write the value only if it is different from the one
			// already stored.
			if hashCmp(r.crem, key, val) == false {
				log.Printf("(remote)->(local) %s %v\n", key, val)
				r.hset(key, val)
			}
		}
	}
}

func (r *Replicator) set(key string, val string) {
	r.crem.Do("SET", key, val)
	r.remKey = key
	r.remVal = val
}

func (r *Replicator) hset(key string, val map[string]interface{}) {
	args := []interface{}{key}
	for hk, hv := range val {
		args = append(args, hk, hv)
	}
	_, e := r.crem.Do("HMSET", args...)
	if e != nil {
		log.Println(e)
	}
	r.remHashKey = key
	r.remHashVal = val
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
