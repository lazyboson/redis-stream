package producer

import (
	"RedisStream/models"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
)

type Producer struct {
	streamName string
}

func NewProducer(streamName string) *Producer {
	return &Producer{streamName: streamName}
}

func (p *Producer) WriteEvents(conn redis.Conn, key string) {
	// Create a new struct
	employee := models.Employee{
		Name:     "ashutosh",
		Employer: "self-employee",
	}
	// Convert struct to JSON
	e, _ := json.Marshal(employee)

	// Send key and value to Redis stream
	_, err := conn.Do("XADD", p.streamName, "*", key, e)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully sent data to Redis stream")
}
