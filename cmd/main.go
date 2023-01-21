package main

import (
	"RedisStream/consumer"
	"RedisStream/pb/apipb/pb"
	"RedisStream/producer"
	"github.com/google/uuid"
	"time"
)

func main() {
	stream := "k-replacement-stream"
	groups := []string{"first-member"}
	p := producer.NewProducer(stream)
	c := consumer.NewConsumer(stream, groups)
	c.CreateConsumerGroup()
	for i := 0; i < 10; i++ {
		id, _ := uuid.NewUUID()
		data := &pb.Employee{
			Id:          id.String(),
			Name:        "ashutosh",
			Designation: "self-employed",
		}
		p.WriteEvents(id.String(), data)
	}
	go c.ReadEventsCons1()
	go c.ReadEventsCons2()

	time.Sleep(10 * time.Second)
}
