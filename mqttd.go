// mqttd, Copyright (c) 2017 Tuomas Starck

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	paho "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/mattn/go-sqlite3"
)

type Counter uint64

type HFPMsg struct {
	VP struct {
		Desi       string  // "I",
		Dir        string  // "1",
		Oper       string  // "-",
		Veh        string  // "H9091",
		Tst        string  // "2017-08-20T10:22:40.000Z", (maybe use time.Time)
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

const sqlInsert = `INSERT OR IGNORE INTO hfp VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

const sqlCreateTable = `CREATE TABLE IF NOT EXISTS hfp (
desi        text,
dir         text,
oper        text,
veh         text,
tst         text,
tsi         integer,
spd         float,
lat         float,
long        float,
odo         float,
oday        text,
jrn         text,
line        text,
start       text,
stop_index  integer,
source      text
)`

var db *sql.DB
var messageCounter Counter

func init() {
	log.SetFlags(log.Lshortfile)

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGHUP)

	go func() {
		for {
			<-signalChannel
			log.Printf("Messages since start: %d\n", messageCounter.value())
		}
	}()

	sqliteOpenAndCreate()
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

func save(topic string, msg HFPMsg) {
	db, err := sql.Open("sqlite3", databaseFilename)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	query, err := db.Prepare(sqlInsert)
	if err != nil {
		log.Fatal(err)
	}

	_, err = query.Exec(
		msg.VP.Desi,
		msg.VP.Dir,
		msg.VP.Oper,
		msg.VP.Veh,
		msg.VP.Tst,
		msg.VP.Tsi,
		msg.VP.Spd,
		msg.VP.Lat,
		msg.VP.Long,
		msg.VP.Odo,
		msg.VP.Oday,
		msg.VP.Jrn,
		msg.VP.Line,
		msg.VP.Start,
		msg.VP.Stop_Index,
		msg.VP.Source,
	)
	if err != nil {
		log.Fatal(err)
	}
}

func messageHandler(c paho.Client, msg paho.Message) {
	var hfp HFPMsg

	if err := json.Unmarshal(msg.Payload(), &hfp); err != nil {
		log.Println(err)
		log.Println(msg.Topic())
		log.Println(string(msg.Payload()))
	}

	messageCounter.increment()
	save(msg.Topic(), hfp)
	/// fmt.Print(".")
	fmt.Println("<<<")
	log.Printf("%+v\n", msg.Topic())
	log.Printf("%+v\n", hfp.VP)
	fmt.Println(">>>")
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

	/// const TOPIC = "/hfp/#"
	const TOPIC = "/hfp/journey/rail/#"
	/// const TOPIC = "/hfp/journey/tram/0040_00409/#"

	opts := paho.NewClientOptions()
	opts = opts.SetConnectionLostHandler(connLostHandler)
	opts = opts.SetDefaultPublishHandler(defPublishHandler)
	opts = opts.SetOnConnectHandler(onConnectHandler)
	opts = opts.AddBroker("tcp://mqtt.hsl.fi:1883")
	/// opts = opts.SetKeepAlive(30 * time.Second)
	/// opts = opts.SetPingTimeout(10 * time.Second)
	/// opts = opts.SetWriteTimeout(30 * time.Second)
	client := paho.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Unable to connect to source")
		log.Println(token.Error())
	}

	var wg sync.WaitGroup
	wg.Add(1)

	token := client.Subscribe(TOPIC, 0, messageHandler)
	if token.Wait() && token.Error() != nil {
		log.Println("Token error")
		log.Println(token.Error())
	}

	wg.Wait()
	os.Exit(0)
}
