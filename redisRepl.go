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
)

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

	// connect to Redis
	c, e := redis.Dial("tcp", *redisAddr)
	if e != nil {
		log.Fatal(e)
	}
	defer c.Close()
	// additional connection to Redis for pubsub
	csub, e := redis.Dial("tcp", *redisAddr)
	if e != nil {
		log.Fatal(e)
	}
	defer csub.Close()
	client := https_client(certFile, keyFile, caFile)
	if *rnam != "" {
		go receive(*httpsAddr, *rnam, client, c)
	}
	// Enable key-event notifications for strings and hashes
	c.Do("CONFIG", "set", "notify-keyspace-events", "K$h")
	send(*httpsAddr, "CONFIG/set/notify-keyspace-events/K$h", client, false)
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
				redis_data := "SET/" + key
				val, _ := redis.String(c.Do("GET", key))
				redis_data = redis_data + "/" + val
				disable_events := false
				if *rnam != "" && strings.HasPrefix(key, *rnam) {
					disable_events = true
				}
				fmt.Printf("(local)->(remote) %s %s\n", key, val)
				send(*httpsAddr, redis_data, client, disable_events)
			}
		case redis.Subscription:
		case error:
			fmt.Println("Error in local Redis subscription")
			return
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
func send(addr, cmd string, c *http.Client, noEvs bool) (string, interface{}) {
	var e error
	// disable events if needed
	if noEvs {
		multi := strings.NewReader("MULTI")
		_, e = c.Post(addr+"/", "text/plain", multi)
		if e != nil {
			log.Fatal(e)
		}
		cf := strings.NewReader("CONFIG/set/notify-keyspace-events/")
		_, e = c.Post(addr+"/", "text/plain", cf)
		if e != nil {
			log.Fatal(e)
		}
	}
	// send the actual command
	resp, e := c.Post(addr+"/", "text/plain", strings.NewReader(cmd))
	defer resp.Body.Close()
	if e != nil {
		log.Fatal(e)
	}
	// re-enable events if needed
	if noEvs {
		cf := strings.NewReader("CONFIG/set/notify-keyspace-events/K$h")
		_, e = c.Post(addr+"/", "text/plain", cf)
		if e != nil {
			log.Fatal(e)
		}
		exec := strings.NewReader("EXEC")
		_, e = c.Post(addr+"/", "text/plain", exec)
		if e != nil {
			log.Fatal(e)
		}
	}
	// return response parsed from JSON
	return parseSingle(json.NewDecoder(resp.Body))
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
			_, val := send(addr, "GET/"+key, client, false)
			fmt.Printf("(remote)->(local) %s %s\n", key, val)
			set(key, rnam, val, c)
		}
	}
}

func set(key, rnam string, v interface{}, c redis.Conn) {
	redisPayload := v.(string)
	if strings.HasPrefix(key, rnam) {
		c.Do("MULTI")
		c.Do("CONFIG", "set", "notify-keyspace-events", "")
	}
	c.Do("SET", key, redisPayload)
	if strings.HasPrefix(key, rnam) {
		c.Do("CONFIG", "set", "notify-keyspace-events", "K$h")
		c.Do("EXEC")
	}
}
