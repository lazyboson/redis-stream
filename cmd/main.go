package main

import (
	"RedisStream/consumer"
	"RedisStream/producer"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/google/uuid"
	"time"
)

func main() {
	// Connect to Redis
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	stream := "newstream"
	groups := []string{"firstgroup", "secondgroup"}
	p := producer.NewProducer(stream)
	c := consumer.NewConsumer(stream, groups)
	for i := 0; i < 10; i++ {
		id, _ := uuid.NewUUID()
		p.WriteEvents(conn, id.String())
	}
	c.CreateConsumerGroup(conn)
	go c.ReadEventsCons1()
	go c.ReadEventsCons2(conn)

	time.Sleep(10 * time.Second)
}
