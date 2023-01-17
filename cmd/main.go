package main

import (
	"RedisStream/consumer"
	"RedisStream/producer"
	"github.com/google/uuid"
	"time"
)

func main() {
	// Connect to Redis
	stream := "kafkareplacementstream"
	groups := []string{"firstgroup"}
	p := producer.NewProducer(stream)
	c := consumer.NewConsumer(stream, groups)
	for i := 0; i < 10; i++ {
		id, _ := uuid.NewUUID()
		p.WriteEvents(id.String())
	}
	c.CreateConsumerGroup()
	go c.ReadEventsCons1()
	go c.ReadEventsCons2()

	time.Sleep(10 * time.Second)
}
