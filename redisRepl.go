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
	"strings"
	"time"
)

var remKey, remVal string

// Main function: connect to local Redis and replicate every 'set' command
// remotely via HHTPS. Optionally, replicate any remote 'set' command
// whose key begins with 'rnam' into the local Redis.
func main() {
	// redis flags
	var redisAddr = flag.String("raddr", ":6379", "Redis server address")
	var rnam = flag.String("rnam", "", "Remote namespace to replicate locally (disabled by default)")
	// https flags
	var httpsAddr = flag.String("haddr", "https://localhost/redis", "HTTPS server address")
	var certFile = flag.String("cert", "someCertFile", "A PEM encoded certificate file.")
	var keyFile = flag.String("key", "someKeyFile", "A PEM encoded private key file.")
	var caFile = flag.String("CA", "someCertCAFile", "A PEM encoded CA's certificate file.")
	flag.Parse()

	// create a Redis connection pool
	pool := redis.Pool{
		MaxIdle: 3,
		Dial: func () (redis.Conn, error) {
			return redis.Dial("tcp", *redisAddr)
		},
	}
	// first connection to Redis, for remote receiver
	crem := pool.Get()
	defer crem.Close()
	// second connection to Redis, for local listener
	cloc := pool.Get()
	defer cloc.Close()
	// additional connection to Redis for pubsub
	csub := pool.Get()
	defer csub.Close()
	client := https_client(certFile, keyFile, caFile)
	// start the local->remote replication task
	ch := make(chan string)
	go checkTimeout(*httpsAddr, client, ch)
	// if needed, start listening for remote events
	if *rnam != "" {
		go receive(*httpsAddr, *rnam, client, crem)
	}
	// Enable key-event notifications for strings and hashes
	cloc.Do("CONFIG", "set", "notify-keyspace-events", "K$h")
	send(*httpsAddr, "CONFIG/set/notify-keyspace-events/K$h", client)
	// subscribe to events from local Redis instance
	fmt.Printf("Replicating local Redis instance to remote address: %s\n", *httpsAddr)
	psc := redis.PubSubConn{Conn: csub}
	psc.PSubscribe("__keyspace@0__:*")
	for {
		switch v := psc.Receive().(type) {
		case redis.PMessage:
			cmd := string(v.Data)
			switch cmd {
			case "set":
				key := strings.TrimPrefix(v.Channel, "__keyspace@0__:")
				val, _ := redis.String(cloc.Do("GET", key))
				if key != remKey || val != remVal {
					escapedKey := strings.Replace(key, "/", "%2f", -1)
					escapedKey = strings.Replace(escapedKey, ".", "%2e", -1)
					escapedVal := strings.Replace(val, "/", "%2f", -1)
					escapedVal = strings.Replace(escapedVal, ".", "%2e", -1)
					ch <- "/" + escapedKey + "/" + escapedVal 
				}
			default:
				fmt.Printf("(local) Received notification for command: %s\n", cmd)
			}
		case redis.Subscription:
		case error:
			fmt.Println("Error in local Redis subscription")
			return
		}
	}
}

func checkTimeout(addr string, c *http.Client, newCmd chan string) {
	timer := time.After(100 * time.Millisecond)
	remRedisCmd := "MSET"
	for {
		select {
		case <-timer:
			// time out, send cmd remotely
			if remRedisCmd != "MSET" {
				numKeys := len(strings.Split(remRedisCmd, "/")) / 2
				fmt.Printf("(local)->(remote) %d keys\n", numKeys)
				send(addr, remRedisCmd, c)
				remRedisCmd = "MSET"
			}
			timer = time.After(100 * time.Millisecond)
		case val := <-newCmd:
			remRedisCmd = remRedisCmd + val
		}
	}
}

// Create a new HTTPS client with the given certificate files.
func https_client(certFile, keyFile, caFile *string) *http.Client {
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
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}
	return client
}

// Send a new command to remote Redis via HTTPS. The command uses the Webdis
// format. If noEvs is true, then a transaction is executed: disable
// events, execute command and then re-enable events. Otherwise, a simple
// command is executed.
func send(addr, cmd string, c *http.Client) (string, interface{}) {
	// send the actual command
	resp, e := c.Post(addr+"/", "text/plain", strings.NewReader(cmd))
	if e != nil {
		log.Fatal(e)
	}
	defer resp.Body.Close()
	// return response parsed from JSON
	k, v := parseSingle(json.NewDecoder(resp.Body))
	ioutil.ReadAll(resp.Body)
	return k, v;
}

// Parse a JSON message, assuming it has a single field (like a reply after
// command execution).
func parseSingle(dec *json.Decoder) (string, interface{}) {
	var rawMsg interface{}
	e := dec.Decode(&rawMsg)
	if e != nil {
		log.Fatal(e)
	}
	m := rawMsg.(map[string]interface{})
	for k, v := range m {
		return k, v
	}
	return "", nil
}

// Receive and handle a message from the remote subscription.
func receive(addr, rnam string, client *http.Client, c redis.Conn) {
	fmt.Printf("Subscribing for remote changes to the namespace: %s\n", rnam)
	resp, e := client.Get(addr + "/PSUBSCRIBE/__keyspace@0__:" + rnam + "*")
	defer resp.Body.Close()
	if e != nil {
		log.Fatal(e)
	}
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
				sub(addr, rnam, client, v, c)
			}
		}
	}
}

func sub(addr, rnam string, client *http.Client, v interface{}, c redis.Conn) {
	redisPayload := v.([]interface{})
	msgType := redisPayload[0].(string)
	if msgType == "pmessage" {
		keySpace := redisPayload[2].(string)
		cmd := redisPayload[3].(string)
		key := strings.TrimPrefix(keySpace, "__keyspace@0__:")
		switch cmd {
		case "set":
			_, val := send(addr, "GET/" + key, client)
			actVal, _ := redis.String(c.Do("GET", key))
			// write the value only if it is different from the one
			// already stored.
			if actVal != val {
				fmt.Printf("(remote)->(local) %s %s\n", key, val)
				set(key, val, c)
			}
		}
	}
}

func set(key string, v interface{}, c redis.Conn) {
	redisPayload := v.(string)
	c.Do("SET", key, redisPayload)
	remKey = key
	remVal = redisPayload
}	
