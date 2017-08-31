// matta, Copyright (c) 2017 Tuomas Starck
/* TODO
 *  refactor sql to another file
 *  should erroneous hfp message be saved?
 *  method to report write interval
 *  method to report messages per write
 *  count how many msgs/sec we get at the moment
 *
 *  tcp://mqtt.hsl.fi:1883
 *  "/hfp/journey/rail/#"
 *  "/hfp/journey/tram/0040_00409/#"
 */

package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/mattn/go-sqlite3"
)

type Counter uint64

type Exit struct{ Code int }

type Config struct {
	debug       bool
	broker_conn string
	mqtt_topic  string
	db_filename string
	chunk_size  uint
	verbose     bool
}

type QueueMsg struct {
	at    time.Time
	qos   int
	topic string
	data  string
}

type HFPMsg struct {
	VP struct {
		Desi       string  // "I",
		Dir        string  // "1",
		Oper       string  // "-",
		Veh        string  // "H9091",
		Tst        string  // "2017-08-20T10:22:40.000Z"
		Tsi        uint64  // 1503224560,
		Spd        float64 // 0,
		Lat        float64 // 60.271046,
		Long       float64 // 24.853437,
		Odo        float64 // 37411,
		Oday       string  // "2017-08-20",
		Jrn        string  // "-",
		Line       string  // "-",
		Start      string  // "1236",
		Stop_Index uint64  // 18,
		Source     string  // "sm5logger"
	}
}

const (
	usageDebug      = "Print out all debug messages"
	usageBrokerConn = "MQTT broker connection string as \"proto://host:port\""
	usageMqttTopic  = "MQTT topic to follow to"
	usageDbFilename = "Name of the database file"
	usageChunkSize  = "Minimum number of messages in one write transaction"
	usageVerbose    = "Verbose output"

	sqlInsert = `INSERT INTO hfp VALUES (?, ?, ?, ?)`
)

const sqlCreateTable = `CREATE TABLE IF NOT EXISTS hfp (
	at    integer,
	qos   integer,
	topic text,
	data  text)`

var conf Config
var db *sql.DB
var insert *sql.Stmt
var messageCounter Counter
var writeBuffer chan QueueMsg

func (c *Counter) value() uint64 {
	return atomic.LoadUint64((*uint64)(c))
}

func (c *Counter) increment() uint64 {
	return atomic.AddUint64((*uint64)(c), 1)
}

func init() {
	log.SetFlags(log.Lshortfile)

	conf = parseArguments()

	writeBuffer = make(chan QueueMsg, 8192)

	hupChannel := make(chan os.Signal, 1)
	signal.Notify(hupChannel, syscall.SIGHUP)

	go func() {
		for {
			<-hupChannel
			log.Printf("Messages since start: %d\n", messageCounter.value())
		}
	}()

	sqliteOpenAndCreate(conf.db_filename)
	writeWaitLoop()
}

func parseArguments() Config {
	ptrDebug := flag.Bool("D", false, usageDebug)
	ptrBrokerConn := flag.String("c", "", usageBrokerConn)
	ptrMqttTopic := flag.String("t", "", usageMqttTopic)
	ptrDbFilename := flag.String("f", "hfp.sqlite", usageDbFilename)
	ptrChunkSize := flag.Uint("s", 1024, usageChunkSize)
	ptrVerbose := flag.Bool("v", false, usageVerbose)

	flag.Parse()

	return Config{
		debug:       *ptrDebug,
		broker_conn: *ptrBrokerConn,
		mqtt_topic:  *ptrMqttTopic,
		db_filename: *ptrDbFilename,
		chunk_size:  *ptrChunkSize,
		verbose:     *ptrVerbose,
	}
}

func sqliteOpenAndCreate(filename string) {
	var err error
	db, err = sql.Open("sqlite3", filename)
	if err != nil {
		log.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(sqlCreateTable)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Database ready")
}

func sqliteClose() {
	db.Close()
	log.Println("Database closed")
}

func sqliteBufferWrite() {
	/// fmt.Print("<#")

	transaction, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer transaction.Rollback()

	statement, err := transaction.Prepare(sqlInsert)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()

loop:
	for {
		select {
		case msg := <-writeBuffer:
			_, err = statement.Exec(msg.at, msg.qos, msg.topic, msg.data)
			if err != nil {
				log.Fatal(err)
			}
		default:
			break loop
		}
	}

	err = transaction.Commit()
	if err != nil {
		log.Fatal(err)
	}

	/// fmt.Print("#>")
}

func writeWaitLoop() {
	defer time.AfterFunc(512*time.Millisecond, writeWaitLoop)
	if len(writeBuffer) >= int(conf.chunk_size) {
		sqliteBufferWrite()
	}
}

func messageHandler(c paho.Client, msg paho.Message) {
	var hfp HFPMsg

	if err := json.Unmarshal(msg.Payload(), &hfp); err != nil {
		log.Println(msg.Topic())
		log.Println(string(msg.Payload()))
		log.Println(err)
	}

	writeBuffer <- QueueMsg{
		at:    time.Now(),
		qos:   int(msg.Qos()),
		topic: msg.Topic(),
		data:  string(msg.Payload())}

	messageCounter.increment()

	/// fmt.Print(".")
	/// fmt.Println("<<<")
	/// log.Printf("%+v\n", msg.Topic())
	/// log.Printf("%+v\n", hfp.VP)
	/// fmt.Println(">>>")
}

func connLostHandler(c paho.Client, e error) {
	log.Println("Client connection lost")
	log.Println(e)
}

/* Default publish handler is called when client receives messages,
 * but there is no handler registered. Such situation may occur e.g.
 * when program is closing.
 */
func defPublishHandler(c paho.Client, m paho.Message) {
	/// log.Printf("%+v\n", m)
	return
}

func onConnectHandler(c paho.Client) {
	log.Println("Client connected")
}

/* Catch SIGTERM or SIGINT and tidy up before exiting.
 */
func exitHandler(client paho.Client) {
	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-exitChannel

		log.Println("Disconnecting client")
		client.Unsubscribe(conf.mqtt_topic)
		client.Disconnect(0)

		log.Println("Flushing write buffer to database")
		sqliteBufferWrite()
		sqliteClose()
		os.Exit(0)
	}()
}

func main() {
	opts := paho.NewClientOptions()
	opts = opts.SetConnectionLostHandler(connLostHandler)
	opts = opts.SetDefaultPublishHandler(defPublishHandler)
	opts = opts.SetOnConnectHandler(onConnectHandler)
	opts = opts.AddBroker(conf.broker_conn)
	client := paho.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Unable to connect to source")
		log.Println(token.Error())
	}

	token := client.Subscribe(conf.mqtt_topic, 0, messageHandler)
	if token.Wait() && token.Error() != nil {
		log.Println("Token error")
		log.Println(token.Error())
	}

	exitHandler(client)

	select {}
}
