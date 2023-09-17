package main

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)
	if err != nil {
		return
	}
	//write a deadline if something went wrong this line will be executed
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))

	conn.WriteMessages(kafka.Message{Value: []byte("Hi, I am a new developer")})
}
