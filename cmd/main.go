package main

import (
	"RedisStream/consumer"
	"RedisStream/pb/apipb/pb"
	"RedisStream/producer"
	"github.com/google/uuid"
	"strconv"
	"time"
)

func main() {
	stream := "lreplacementstream"
	groups := []string{"first-member"}
	p := producer.NewProducer(stream)
	c := consumer.NewConsumer(stream, groups)
	c.CreateConsumerGroup()
	for i := 0; i < 100; i++ {
		id, _ := uuid.NewUUID()
		data := &pb.Employee{
			Id:          strconv.Itoa(i) + "_" + id.String(),
			Name:        "ashutosh",
			Designation: "self-employed",
		}
		p.WriteEvents(id.String(), data)
	}
	//go c.ReadEventsCons1()
	go c.ReadEventsCons2()

	time.Sleep(10 * time.Second)
}
