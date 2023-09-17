package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)
	if err != nil {
		return
	}
	//write a deadline if something went wrong this line will be executed
	conn.SetReadDeadline(time.Now().Add(time.Second * 3))
	/*
		for {
			message, err := conn.ReadMessage(1e6)
			if err != nil {
				return
			}
			fmt.Println(string(message.Value))
		}
	*/
	//other approach
	batch := conn.ReadBatch(1e3, 1e9) // 1e3 = 1000
	// array of bytes
	bytes := make([]byte, 1e3)

	for {
		_, err := batch.Read(bytes)
		if err != nil {
			// the following message could not be read (because no other message) so it's better to break
			break
		}
		fmt.Println(string(bytes))

	}

}
