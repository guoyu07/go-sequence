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

var redisPool *redis.Pool

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

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))  // set 5 seconds timeout

	for {
		line, err := bufio.NewReader(conn).ReadBytes('\n')

		if err != nil {
			fmt.Println(err)
			return
		}
		
		content := strings.TrimRight(string(line), "\n")
		fmt.Println("We get" + content)

		dec := json.NewDecoder(strings.NewReader(content))
		var conf Conf
		if err := dec.Decode(&conf); err == io.EOF {
			fmt.Println("Json parse error ... " + content)
			return
		}

		fmt.Println("AppName: " + conf.AppName + ", IdQueue: " + conf.IdQueue + ", Step: " + strconv.Itoa(conf.Step))

		redisConn := redisPool.Get()

		go generate(redisConn, conf, out)

		ticker := time.NewTicker(time.Millisecond * 100) // 3 Senconds
		go func() {
			for t := range ticker.C {
				l, err := redis.Int(redisConn.Do("llen", getRedisKey(conf.AppName, conf.IdQueue)))
				if err != nil {
					fmt.Println("Redis do `llen` failed ... `" + getRedisKey(conf.AppName, conf.IdQueue) + "` ... ")
					break
				}

				fmt.Println("Redis return `llen`: ", strconv.Itoa(l))

				if l < 5 {
					push(redisConn, conf)
				}
				fmt.Println("Tick at", t)
			}
		}()
	}
}

func send(conn net.Conn, in <- chan string) {
	defer conn.Close()

	message := <- in
	log.Print(message)
	io.Copy(conn, bytes.NewBufferString(message))
}

func generate(redisConn redis.Conn, conf Conf, out chan string) {
	redisKey := getRedisKey(conf.AppName, conf.IdQueue)
	n, err := redis.Int(redisConn.Do("LPOP", redisKey))
	if err != nil {
		fmt.Println("Noting to POP In Redis in " + redisKey )
		// handle error
	}
	str := strconv.Itoa(n)
	fmt.Println("LPOP " + redisKey + "Value " + str)
	out <- str + "\n"

}

func push(redisConn redis.Conn, conf Conf) bool {
	mysqlConn := OpenDB()
	defer mysqlConn.Close()

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
		redisConn.Send("RPUSH", getRedisKey(conf.AppName, conf.IdQueue), lastSeq - i + 1)
	}
	_, err1 := redisConn.Do("EXEC");
	if(err1 != nil) {
		fmt.Println("redis transaction failed")
		return false
	}
	return true
}

func OpenRedis() redis.Conn {
	conn, err := redis.Dial("tcp", redisSource)
	conn.Do("SELECT", 0)
	if err != nil {
		panic(err)
	}
	return conn
}

func OpenDB() *sql.DB {
	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		panic(err)
	}
	return db
}

func getRedisKey(appName string, idQueue string) string {
	return redisNS + appName + ":" + idQueue
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
