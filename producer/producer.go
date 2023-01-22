package producer

import (
	"RedisStream/pb/apipb/pb"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"google.golang.org/protobuf/proto"
)

type Producer struct {
	streamName string
}

func NewProducer(streamName string) *Producer {
	return &Producer{streamName: streamName}
}

func (p *Producer) WriteEvents(key string, data *pb.Employee) {
	// Create a new struct
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	e, err := proto.Marshal(data)
	if err != nil {
		fmt.Println(err)
	}
	// Send key and value to Redis stream
	_, err = conn.Do("XADD", p.streamName, "MAXLEN", "~", "100000", "*", []byte(key), e)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully sent data to Redis stream")
}
