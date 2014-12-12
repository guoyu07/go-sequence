package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"bytes"
	"bufio"
	"strconv"
	"encoding/json"
	"log"
	"net"
	"time"

	"github.com/garyburd/redigo/redis"
        _ "github.com/go-sql-driver/mysql"
)

const dataSource = "root@tcp(127.0.0.1:3306)/test"
const redisSource = "127.0.0.1:6379"
const redisNS = "SeqGend:"

type Conf struct {
	AppName, IdQueue string
	Step int
}

type QueueState struct {
	RedisKey string
	LastTime time.Time
	Ticker *time.Ticker
}

var redisPool *redis.Pool
var mysqlConn *sql.DB
var QueueStateMap map[string]*QueueState

func newPool(server, password string) *redis.Pool {
    return &redis.Pool{
        MaxIdle: 3,
        IdleTimeout: 240 * time.Second,
        Dial: func () (redis.Conn, error) {
            c, err := redis.Dial("tcp", server)
            if err != nil {
                return nil, err
            }
	    // if _, err := c.Do("AUTH", password); err != nil {
            //    c.Close()
            //    return nil, err
            // }
            return c, err
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
    }
}

func main() {
	service := "0.0.0.0:6381"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	fmt.Println("Listen on: ", tcpAddr.IP.String(), ":", tcpAddr.Port)

	redisPool = newPool(redisSource, "")
	mysqlConn = OpenDB()
	QueueStateMap = make(map[string]*QueueState)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Listen " + service + "failed ...")
			return
		}
		fmt.Println("Accept from: ", conn.RemoteAddr().String())

		channel := make(chan string)
		go handle(conn, channel)
		go send(conn, channel)
	}
}

func handle(conn net.Conn, out chan string) {
	
	defer close(out)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))  // set 2 seconds timeout

	for {
		line, err := bufio.NewReader(conn).ReadBytes('\n')
		
		if err != nil {
			log.Println("Client close connection:", conn.RemoteAddr().String())
			return
		}

		content := strings.TrimRight(string(line), "\n")

		log.Println("We get" + content)

		dec := json.NewDecoder(strings.NewReader(content))
		var conf Conf
		if err := dec.Decode(&conf); err == io.EOF {
			fmt.Println("Json parse error ... " + content)
			return
		}

		log.Println("AppName: " + conf.AppName + ", IdQueue: " + conf.IdQueue + ", Step: " + strconv.Itoa(conf.Step))

		go generate(conf, out)

		redisKey := getRedisKey(conf)
		
		_, ok := QueueStateMap[redisKey]
		if ok == false{
			log.Println("New Queue add to Map ...")
			QueueStateMap[redisKey] = &QueueState{RedisKey: redisKey, LastTime: time.Now(), Ticker: time.NewTicker(time.Millisecond * 100)}
		}
		
		queueStateChannel := queueMonitor(conf)
		queueStateChannel <- QueueStateMap[redisKey]
	}
}

func send(conn net.Conn, in <- chan string) {
	defer conn.Close()
	message := <- in
	log.Print("Sending data to client ... ", message)
	io.Copy(conn, bytes.NewBufferString(message))
}

func generate(conf Conf, out chan string) {
	redisConn := redisPool.Get()
	redisKey := getRedisKey(conf)
	n, err := redis.Int(redisConn.Do("LPOP", redisKey))
	if err != nil {
		log.Println("Noting to POP In Redis in " + redisKey )
		// handle error
	}
	str := strconv.Itoa(n)
	log.Println("LPOP " + redisKey + "Value " + str)
	out <- str + "\n"
}

func push(conf Conf) bool {
	redisConn := redisPool.Get()

	mysqlConn.Exec("UPDATE `seq` SET `seq`=LAST_INSERT_ID(`seq`+?) WHERE `app`=? AND `bucket`=?", conf.Step, conf.AppName, conf.IdQueue)
	var lastSeq int
	err := mysqlConn.QueryRow("SELECT LAST_INSERT_ID()").Scan(&lastSeq)
	if err != nil {
		fmt.Println("select last_insert_id failed")
		return false
	}
	fmt.Println("Last insert id: ", lastSeq)
	redisConn.Send("MULTI")
	for i := conf.Step; i > 0; i-- {
		redisConn.Send("RPUSH", getRedisKey(conf), lastSeq - i + 1)
	}
	_, err1 := redisConn.Do("EXEC");
	if(err1 != nil) {
		fmt.Println("redis transaction failed")
		return false
	}
	return true
}

func queueMonitor(conf Conf) chan <- *QueueState {
	redisConn := redisPool.Get()
	accessQueueState := make(chan *QueueState)
	redisKey := getRedisKey(conf)
	go func() {
		for {
			select {
			case t := <- QueueStateMap[redisKey].Ticker.C:
				l, err := redis.Int(redisConn.Do("llen", redisKey))
				if err != nil {
					log.Println("Redis do `llen` failed ... `" + redisKey + "` ... ")
					break
				}
				log.Println("Redis return `llen`: ", redisKey, strconv.Itoa(l))
				if l < 5 {
					push(conf)
				}
				log.Println("Tick at", t)

			case <- accessQueueState:
				QueueStateMap[redisKey].LastTime = time.Now()
				for _, queueState := range QueueStateMap {
					if queueState.LastTime.Unix() + 60 < time.Now().Unix() {
						log.Println("Long time no acess, stop ticker", queueState.RedisKey)
						queueState.Ticker.Stop()
					}
				}
				log.Println("Queue Map Content:", QueueStateMap)
			}
		}
	}()

	return accessQueueState
}

func OpenDB() *sql.DB {
	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		log.Fatalln("Db error ...")
		panic(err)
	}
	db.SetMaxOpenConns(100)
	return db
}

func getRedisKey(conf Conf) string {
	return redisNS + conf.AppName + ":" + conf.IdQueue
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
