package consumer

import (
	"RedisStream/models"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
)

type Consumer struct {
	streamName string
	groupName  []string
}

func NewConsumer(stream string, groups []string) *Consumer {
	return &Consumer{
		streamName: stream,
		groupName:  groups,
	}
}

type Entry struct {
	ID     string
	Fields map[string][]byte
}

// Entries is a helper that converts an array of stream entries into Entry values.
// Requires two values in each entry result, and an even number of field values.
func entries(reply interface{}, err error) ([]Entry, error) {
	vs, err := redis.Values(reply, err)
	if err != nil {
		return nil, err
	}

	entries := make([]Entry, len(vs))
	for i, v := range vs {
		evs, ok := v.([]interface{})
		if !ok || len(evs) != 2 {
			return nil, errors.New("redigo: Entry expects two value result")
		}
		id, err := redis.String(evs[0], nil)
		if err != nil {
			return nil, err
		}
		sm, err := StringBytes(evs[1], nil)
		if err != nil {
			return nil, err
		}
		entries[i] = Entry{
			ID:     id,
			Fields: sm,
		}
	}
	return entries, nil
}

func (c *Consumer) ReadEventsCons1() {
	// Connect to Redis
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	for {
		// Read key and value from Redis stream
		reply, err := conn.Do("XREADGROUP", "GROUP", c.groupName[0], "ashu", "COUNT", "1", "STREAMS", c.streamName, ">")
		vs, err := redis.Values(reply, err)
		if err != nil {
			if errors.Is(err, redis.ErrNil) {
				continue
			}
			fmt.Printf("Error: %+v", err)
		}

		// Get the first and only value in the array since we're only
		// reading from one stream "some-stream-name" here.
		vs, err = redis.Values(vs[0], nil)
		if err != nil {
			fmt.Printf("Error: %+v", err)
		}

		// Ignore the stream name as the first value as we already have
		// that in hand! Just get the second value which is guaranteed to
		// exist per the docs, and parse it as some stream entries.
		res, err := entries(vs[1], nil)
		if err != nil {
			fmt.Errorf("error parsing entries: %w", err)
		}
		for _, val := range res {
			for k, v := range val.Fields {
				empl := &models.Employee{}
				_ = json.Unmarshal(v, empl)
				fmt.Printf("From Consumer Ashu:  Key: %s and val: %+v \n", k, empl)
			}
			reply, err := redis.Int(conn.Do("XACK", c.streamName, c.groupName[0], val.ID))
			if reply != 1 {
				fmt.Printf("failed to ack: err: %+v", err)
			}
		}

	}
}

func (c *Consumer) ReadEventsCons2(conn redis.Conn) {
	// Connect to Redis
	for {
		// Read key and value from Redis stream
		reply, err := conn.Do("XREADGROUP", "GROUP", c.groupName[0], "pandey", "COUNT", "1", "STREAMS", c.streamName, ">")
		vs, err := redis.Values(reply, err)
		if err != nil {
			if errors.Is(err, redis.ErrNil) {
				continue
			}
			fmt.Printf("Error: %+v", err)
		}

		// Get the first and only value in the array since we're only
		// reading from one stream "some-stream-name" here.
		vs, err = redis.Values(vs[0], nil)
		if err != nil {
			fmt.Printf("Error: %+v", err)
		}

		// Ignore the stream name as the first value as we already have
		// that in hand! Just get the second value which is guaranteed to
		// exist per the docs, and parse it as some stream entries.
		res, err := entries(vs[1], nil)
		if err != nil {
			fmt.Errorf("error parsing entries: %w", err)
		}

		for _, val := range res {
			for k, v := range val.Fields {
				empl := &models.Employee{}
				_ = json.Unmarshal(v, empl)
				fmt.Printf("From Consumer Pandey:  Key: %s and val: %+v \n", k, empl)
			}
			reply, err := redis.Int(conn.Do("XACK", c.streamName, c.groupName[0], val.ID))
			if reply != 1 {
				fmt.Printf("failed to ack: err: %+v", err)
			}
		}
	}
}

func (c *Consumer) CreateConsumerGroup(conn redis.Conn) {
	for _, val := range c.groupName {
		_, err := redis.Values(conn.Do("XGROUP", "CREATE", c.streamName, val, "$"))
		if err != nil {
			if err.Error() == "BUSYGROUP Consumer Group name already exists" {
				fmt.Printf("Consumer Group already exist: skipping creation\n")
			} else {
				fmt.Printf("failed to create consumer group: %+v \n", err)
			}
		} else {
			fmt.Println("consumer group created successfully")
		}
	}
}

// StringBytes is a helper that converts an array of strings (alternating key, value)
// into a map[string]string. The HGETALL and CONFIG GET commands return replies in this format.
// Requires an even number of values in result.
func StringBytes(result interface{}, err error) (map[string][]byte, error) {
	values, err := redis.Values(result, err)
	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("redigo: StringMap expects even number of values result")
	}
	m := make(map[string][]byte, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, okKey := values[i].([]byte)
		value, okValue := values[i+1].([]byte)
		if !okKey || !okValue {
			return nil, errors.New("redigo: StringMap key not a bulk string value")
		}
		m[string(key)] = value
	}
	return m, nil
}
