package main

import (
	"fmt"
	redis "github.com/garyburd/redigo/redis"
	"flag"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"encoding/json"
	"io"
)

func main() {
	// redis flags
	var redisAddr = flag.String("raddr", ":6379", "Redis server address")
	var locNam = flag.String("locNam", "restRI", "Namespace to use for your RI")
	// https flags
	var httpsAddr = flag.String("haddr", "https://localhost/redis", "HTTPS server address")
	var certFile = flag.String("cert", "someCertFile", "A PEM encoded certificate file.")
	var keyFile  = flag.String("key", "someKeyFile", "A PEM encoded private key file.")
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
	go https_receive(httpsAddr, client, c, locNam);
	// Enable key-event notifications for strings and hashes
	c.Do("CONFIG", "set", "notify-keyspace-events", "K$h");
	https_send(httpsAddr, client, "CONFIG/set/notify-keyspace-events/K$h", false) 
	// subscribe to events from local Redis instance
	fmt.Printf("Exposing local Redis instance with namespace: %s\n", *locNam)	
	psc := redis.PubSubConn{Conn: csub}
	psc.PSubscribe("__keyspace@0__:*")
	for {
		switch v := psc.Receive().(type) {
		case redis.PMessage:
			cmd := string(v.Data) 
			switch cmd {
			case "set":
				key := strings.TrimPrefix(v.Channel, "__keyspace@0__:");
				fmt.Printf("Local set command: %s\n", key)
				redis_data := "SET/" + key
				val, _ := redis.String(c.Do("GET", key))
				redis_data = redis_data + "/" + val
				disable_events := false
				if strings.HasPrefix(key, *locNam) {
					disable_events = true
				}
				https_send(httpsAddr, client, redis_data, disable_events)
			}
		case redis.Subscription:
		case error:
			fmt.Println("Error in local Redis subscription");
			return 
		}
	}
}

func https_client(certFile *string, keyFile *string, caFile *string) *http.Client {
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
	return client;
}

func https_send(httpsAddr *string, client *http.Client, redis_data string, disable_events bool) (string, interface{}) {
	var err error
	if disable_events {
		// execute a transaction: disable events, execute command and then re-enable events
		_, err = client.Post(*httpsAddr + "/", "text/plain", strings.NewReader("MULTI"))
		if err != nil {
			log.Fatal(err)
		}
		_, err = client.Post(*httpsAddr + "/", "text/plain", strings.NewReader("CONFIG/set/notify-keyspace-events/"))
		if err != nil {
			log.Fatal(err)
		}
	}
	resp, err := client.Post(*httpsAddr + "/", "text/plain", strings.NewReader(redis_data))
	if err != nil {
		log.Fatal(err)
	}
	if disable_events {
		_, err = client.Post(*httpsAddr + "/", "text/plain", strings.NewReader("CONFIG/set/notify-keyspace-events/K$h"))
		if err != nil {
			log.Fatal(err)
		}
		_, err = client.Post(*httpsAddr + "/", "text/plain", strings.NewReader("EXEC"))
		if err != nil {
			log.Fatal(err)
		}
	}
	defer resp.Body.Close()
	return parseSingle(json.NewDecoder(resp.Body))
}

func https_receive(httpsAddr *string, client *http.Client, c redis.Conn, locNam *string) {
	fmt.Printf("subscribing for remote changes to the namespace: %s\n", *locNam)	
	resp, err := client.Get(*httpsAddr + "/PSUBSCRIBE/__keyspace@0__:" + *locNam + "*")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	parseMsg(httpsAddr, client, resp.Body, c)
}

func parseSingle(dec *json.Decoder) (string, interface{}) {
	var rawMsg interface{}
	err := dec.Decode(&rawMsg)
	if err != nil {
		log.Fatal(err)
	}
	m := rawMsg.(map[string]interface{})
	for k, v := range m {
		return k, v
	}
	return "", nil
}

func parseMsg(httpsAddr *string, client *http.Client, reader io.Reader, c redis.Conn) {
	dec := json.NewDecoder(reader)
	for dec.More() {
		var rawMsg interface{}
		err := dec.Decode(&rawMsg)
		fmt.Println(rawMsg)
		if err != nil {
			log.Fatal(err)
		}
		m := rawMsg.(map[string]interface{})
		for k, v := range m {
			switch k {
			case "PSUBSCRIBE":
				sub(httpsAddr, client, v, c)
			}
		}
	}
}

func sub(httpsAddr *string, client *http.Client, v interface{}, c redis.Conn) {
	redisPayload := v.([]interface{})
	msgType := redisPayload[0].(string)
	if msgType == "pmessage" {
		keySpace := redisPayload[2].(string)
		cmd := redisPayload[3].(string)
		key := strings.TrimPrefix(keySpace, "__keyspace@0__:");
		switch cmd {
		case "set":
			_, v := https_send(httpsAddr, client, "GET/" + key, false)
			fmt.Printf("GET result: %v\n", v)
			set(key, v, c)
		}
	}
}
	
func set(k string, v interface{}, c redis.Conn) {
	redisPayload := v.(string)
	c.Do("SET", k, redisPayload)
}



