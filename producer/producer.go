package producer

import (
	"RedisStream/models"
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
)

type Producer struct {
	streamName string
}

func NewProducer(streamName string) *Producer {
	return &Producer{streamName: streamName}
}

func (p *Producer) WriteEvents(key string) {
	// Create a new struct
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	employee := models.Employee{
		Name:     "ashutosh",
		Employer: "self-employee",
	}
	// Convert struct to JSON
	e, _ := json.Marshal(employee)

	// Send key and value to Redis stream
	_, err = conn.Do("XADD", p.streamName, "*", key, e)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully sent data to Redis stream")
}
