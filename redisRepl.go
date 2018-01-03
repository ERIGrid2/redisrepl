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
	"fmt"
	redis "github.com/garyburd/redigo/redis"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
	"syscall"
	"golang.org/x/crypto/ssh/terminal"
	"bytes"
)

const version = "master"
// logger for printing messages with microsecond precision
var logger = log.New(os.Stderr, "", log.Ldate | log.Lmicroseconds | log.LUTC)

// Replicator is a remote Redis replicator. It replicates every 'set' or 'hset'
// command from a local Redis instance to a remote one, using the Webdis
// protocol. Optionally, it can also work in the reverse direction.
type Replicator struct {
	httpsAddr         string         // HTTPS server address
	client            *http.Client   // HTTPS client
	cloc, csub, crem  redis.Conn     // Redis connections
	locExpectedEvents map[string]int // events expected from local Redis
	locMutex          sync.Mutex     // protects access to locExpectedEvents
	remExpectedEvents map[string]int // events expected from remote Redis
	remMutex          sync.Mutex     // protects access to remExpectedEvents
}

// ReplOpt contains options to be used by a Replicator.
type ReplOpt struct {
	redisAddr string       // Local Redis server address
	redisPass string       // Local Redis server password
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
	var redisPwEn = flag.Bool("pass", false, "Use password for Redis (disabled by default)")
	// https flags
	var httpsAddr = flag.String("haddr", "https://localhost/redis", "HTTPS server address")
	flag.Parse()
	if *showVersion {
		logger.Printf("RedisRepl version: %s\n", version)
		os.Exit(0)
	}
	// ask for password if needed
	var redisPw string
	if *redisPwEn == true {
		fmt.Print("Enter Redis password: ")
		bytePassword, _ := terminal.ReadPassword(int(syscall.Stdin))
		redisPw = string(bytePassword)
	}
	// build replicator options
	client := https_client(*certFile, *keyFile, *caFile)
	opts := ReplOpt{
		redisAddr: *redisAddr,
		redisPass: redisPw,
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
// 4. optionally, replicate any remote 'set', 'hset' or 'hmset' command whose key
//    begins with <nam> into the local Redis.
func (r *Replicator) Start(opts ReplOpt) {
	r.locExpectedEvents = make(map[string]int)
	r.remExpectedEvents = make(map[string]int)
	r.httpsAddr = opts.httpsAddr
	r.client = opts.client
	// create a Redis connection pool
	pool := redis.Pool{
		MaxIdle: 3,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", opts.redisAddr)
			if err != nil {
				return nil, err
			}
			if opts.redisPass != "" {
				if _, err := c.Do("AUTH", opts.redisPass); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, nil
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
	logger.Printf("Replicating local Redis instance to remote address: %s\n", r.httpsAddr)
	psc := redis.PubSubConn{Conn: r.csub}
	psc.PSubscribe("__keyspace@0__:" + opts.nam + "*")
	for {
		switch v := psc.Receive().(type) {
		case redis.PMessage:
			cmd := string(v.Data)
			key := strings.TrimPrefix(v.Channel, "__keyspace@0__:")
			r.handleCmd(cmd, key)
		case redis.Subscription:
		case error:
			logger.Println("Error in local Redis subscription")
			return
		}
	}
}

func (r *Replicator) handleCmd(cmd, key string) {
	switch cmd {
	case "set":
		// the maps need a mutex
		r.locMutex.Lock()
		if r.locExpectedEvents[key] == 0 {
			val, e := redis.String(r.cloc.Do("GET", key))
			if e != nil {
				logger.Printf("Error reading %s from Redis\n", key)
			}
			// Using WriteString is much faster than
			// concatenation with '+'.
			var buffer bytes.Buffer
			buffer.WriteString("SET/")
			buffer.WriteString(escape(key))
			buffer.WriteString("/")
			buffer.WriteString(escape(val))
			redisData := buffer.String()
			r.locMutex.Unlock()
			r.remMutex.Lock()
			r.remExpectedEvents[key] = r.remExpectedEvents[key] + 1
			r.remMutex.Unlock()
			logger.Printf("(local)->(remote) %s %s\n", key, val)
			send(r.httpsAddr, redisData, r.client)
		} else {
			r.locExpectedEvents[key] = r.locExpectedEvents[key] - 1
			r.locMutex.Unlock()
		}
	case "hset":
		// the maps need a mutex
		r.locMutex.Lock()
		if r.locExpectedEvents[key] == 0 {
			fields, e := redis.StringMap(r.cloc.Do("HGETALL", key))
			if e != nil {
				logger.Printf("Error reading %s from Redis\n", key)
			}
			// Using WriteString is much faster than
			// concatenation with '+'.
			var buffer bytes.Buffer
			buffer.WriteString("HMSET/" + key)
			for k, v := range fields {
				buffer.WriteString("/")
				buffer.WriteString(escape(k))
				buffer.WriteString("/")
				buffer.WriteString(escape(v))
			}
			redisData := buffer.String()
			r.locMutex.Unlock()
			r.remMutex.Lock()
			r.remExpectedEvents[key] = r.remExpectedEvents[key] + 1
			r.remMutex.Unlock()
			logger.Printf("(local)->(remote) hash %s\n", key)
			send(r.httpsAddr, redisData, r.client)
		} else {
			r.locExpectedEvents[key] = r.locExpectedEvents[key] - 1
			r.locMutex.Unlock()
		}
	default:
		logger.Printf("(local) Unhandled event: %s\n", cmd)
	}
}

// Create a new HTTPS client with the given certificate files.
func https_client(certFile, keyFile, caFile string) *http.Client {
	// Load client cert
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		logger.Fatal(err)
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		logger.Fatal(err)
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
		logger.Println(e)
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
		logger.Println(e)
		return "", e
	}
	for k, v := range rawMsg {
		return k, v
	}
	return "", nil
}

// Receive and handle a message from the remote subscription.
func (r *Replicator) receive(nam string) {
	logger.Printf("Subscribing for remote changes to the namespace: %s\n", nam)
	// Enable key-event notifications for strings and hashes
	send(r.httpsAddr, "CONFIG/set/notify-keyspace-events/K$h", r.client)

	resp, e := r.client.Get(r.httpsAddr + "/PSUBSCRIBE/__keyspace@0__:" + nam + "*")
	if e != nil {
		logger.Println(e)
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
			logger.Fatal(err)
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
	logger.Println("Error: remote HTTPS connection down, retrying in 10 seconds")
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
			// write the value only if it is different from the one
			// already stored.
			// the maps need a mutex
			r.remMutex.Lock()
			if r.remExpectedEvents[key] == 0 {
				_, res := send(r.httpsAddr, "GET/"+escape(key), r.client)
				val := res.(string)
				logger.Printf("(remote)->(local) %s %s\n", key, val)
				r.remMutex.Unlock()
				r.locMutex.Lock()
				r.locExpectedEvents[key] = r.locExpectedEvents[key] + 1
				r.locMutex.Unlock()
				r.set(key, val)
			} else {
				r.remExpectedEvents[key] = r.remExpectedEvents[key] - 1
				r.remMutex.Unlock()
			}
		case "hset":
			// write the value only if it is different from the one
			// already stored.
			// the maps need a mutex
			r.remMutex.Lock()
			if r.remExpectedEvents[key] == 0 {
				_, res := send(r.httpsAddr, "HGETALL/"+escape(key), r.client)
				val := res.(map[string]interface{})
				logger.Printf("(remote)->(local) hash %s\n", key)
				r.remMutex.Unlock()
				r.locMutex.Lock()
				r.locExpectedEvents[key] = r.locExpectedEvents[key] + 1
				r.locMutex.Unlock()
				r.hset(key, val)
			} else {
				r.remExpectedEvents[key] = r.remExpectedEvents[key] - 1
				r.remMutex.Unlock()
			}
		}
	}
}

func (r *Replicator) set(key string, val string) {
	r.crem.Do("SET", key, val)
}

func (r *Replicator) hset(key string, val map[string]interface{}) {
	args := []interface{}{key}
	for hk, hv := range val {
		args = append(args, hk, hv)
	}
	_, e := r.crem.Do("HMSET", args...)
	if e != nil {
		logger.Println(e)
	}
}

func escape(msg string) string {
	escaped := url.QueryEscape(msg)
	// in Webdis we must escape also the point
	escaped = strings.Replace(escaped, ".", "%2e", -1)
	return escaped
}
