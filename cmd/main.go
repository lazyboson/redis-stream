package main

import (
	"RedisStream/consumer"
	"RedisStream/producer"
	"fmt"
	"github.com/gomodule/redigo/redis"
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
	stream := "secondstream"
	groups := []string{"firstgroup"}
	p := producer.NewProducer(stream)
	c := consumer.NewConsumer(stream, groups)
	for i := 0; i < 10; i++ {
		id, _ := uuid.NewUUID()
		p.WriteEvents(conn, id.String())
	}
	c.CreateConsumerGroup()
	go c.ReadEventsCons1()
	go c.ReadEventsCons2()

	time.Sleep(10 * time.Second)
}
