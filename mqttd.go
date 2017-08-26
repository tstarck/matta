// mqttd, Copyright (c) 2017 Tuomas Starck
/* TODO
 *  db filename as cli arg
 *  refactor sql to another file
 *  should erroneous hfp message be saved?
 */

package main

import (
	"database/sql"
	"encoding/json"
	/// "fmt"
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

const databaseFilename = "./hfp.sqlite"

const sqlInsert = `INSERT INTO hfp VALUES (?, ?, ?, ?)`

const sqlCreateTable = `CREATE TABLE IF NOT EXISTS hfp (
	at    integer,
	qos   integer,
	topic text,
	data  text)`

var db *sql.DB
var insert *sql.Stmt
var messageCounter Counter
var writeBuffer chan QueueMsg

func init() {
	log.SetFlags(log.Lshortfile)

	writeBuffer = make(chan QueueMsg, 8192)

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGHUP)

	go func() {
		for {
			<-signalChannel
			log.Printf("Messages since start: %d\n", messageCounter.value())
		}
	}()

	sqliteOpenAndCreate()
	writeWaitLoop()
}

func (c *Counter) value() uint64 {
	return atomic.LoadUint64((*uint64)(c))
}

func (c *Counter) increment() uint64 {
	return atomic.AddUint64((*uint64)(c), 1)
}

func sqliteOpenAndCreate() {
	var err error
	db, err = sql.Open("sqlite3", databaseFilename)
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
	log.Println("Database opened")
}

func sqliteClose() {
	db.Close()
	log.Println("Database closed")
}

func sqliteBufferWrite() {
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
}

func writeWaitLoop() {
	defer time.AfterFunc(1000*time.Millisecond, writeWaitLoop)
	if len(writeBuffer) >= 100 {
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
	log.Println("Connection lost")
	log.Println(e)
}

func defPublishHandler(c paho.Client, m paho.Message) {
	log.Println("Default publish handler called")
	log.Printf("%+v\n", m)
}

func onConnectHandler(c paho.Client) {
	log.Println("Client connected")
}

func main() {
	defer sqliteClose()

	const TOPIC = "/hfp/#"
	/// "/hfp/journey/rail/#"
	/// "/hfp/journey/tram/0040_00409/#"

	opts := paho.NewClientOptions()
	opts = opts.SetConnectionLostHandler(connLostHandler)
	opts = opts.SetDefaultPublishHandler(defPublishHandler)
	opts = opts.SetOnConnectHandler(onConnectHandler)
	opts = opts.AddBroker("tcp://mqtt.hsl.fi:1883")
	client := paho.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Unable to connect to source")
		log.Println(token.Error())
	}

	token := client.Subscribe(TOPIC, 0, messageHandler)
	if token.Wait() && token.Error() != nil {
		log.Println("Token error")
		log.Println(token.Error())
	}

	select {}
}
