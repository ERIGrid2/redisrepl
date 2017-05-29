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
	var locHash = flag.String("lh", "meas", "Redis local hash to replicate remotely")
	var remHash = flag.String("rh", "ctrl", "Redis remote hash to replicate locally")
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
	go https_receive(httpsAddr, client, c, remHash);
	// Enable key-event notifications for strings and hashes
	c.Do("CONFIG", "set", "notify-keyspace-events", "KA");
	https_send(httpsAddr, client, "CONFIG/set/notify-keyspace-events/KA") 
	// subscribe to events from local Redis instance
	fmt.Printf("subscribing for changes to the local hash: %s\n", *locHash)	
	psc := redis.PubSubConn{Conn: csub}
	psc.Subscribe("__keyspace@0__:" + *locHash)
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			cmd := string(v.Data) 
			switch cmd {
			case "hset":
				key := strings.Split(v.Channel, ":")[1];
				fields, _ := redis.StringMap(c.Do("HGETALL", key))
				redis_data := "HMSET/" + key
				for k, v := range fields {
					redis_data = redis_data + "/" + k
					redis_data = redis_data + "/" + v
				}
				https_send(httpsAddr, client, redis_data)
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

func https_send(httpsAddr *string, client *http.Client, redis_data string) (string, interface{}) {
	resp, err := client.Post(*httpsAddr + "/", "text/plain", strings.NewReader(redis_data))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	return parseSingle(json.NewDecoder(resp.Body))
}

func https_receive(httpsAddr *string, client *http.Client, c redis.Conn, remHash *string) {
	fmt.Printf("subscribing for changes to the remote hash: %s\n", *remHash)	
	resp, err := client.Get(*httpsAddr + "/SUBSCRIBE/__keyspace@0__:" + *remHash)
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
			case "SUBSCRIBE":
				sub(httpsAddr, client, v, c)
			}
		}
	}
}

func sub(httpsAddr *string, client *http.Client, v interface{}, c redis.Conn) {
	redisPayload := v.([]interface{})
	msgType := redisPayload[0].(string)
	if msgType == "message" {
		keySpace := redisPayload[1].(string)
		cmd := redisPayload[2].(string)
		key := strings.Split(keySpace, ":")[1];
		switch cmd {
		case "hset":
			_, v := https_send(httpsAddr, client, "HGETALL/" + key)
			hset(key, v, c)
		}
	}
}
	
func hset(k string, v interface{}, c redis.Conn) {
	redisPayload := v.(map[string]interface{})
	for pk, pv := range redisPayload {
		c.Do("HSET", k, pk, pv)
	}
}



