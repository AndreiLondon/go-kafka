package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Create a connection to the Kafka broker
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)
	if err != nil {
		log.Fatalf("failed to dial leader: %v", err)
	}
	defer conn.Close() // Ensure connection is closed

	// Set a read deadline to prevent indefinite waits
	conn.SetReadDeadline(time.Now().Add(time.Second * 3))

	// Alternative approach to read messages in batches
	batch := conn.ReadBatch(1e3, 1e9) // Max size: 1KB per message, 1GB for the batch
	defer batch.Close()               // Ensure batch is closed

	bytes := make([]byte, 1e3) // Buffer to hold incoming messages

	for {
		_, err := batch.Read(bytes)
		if err != nil {
			log.Printf("failed to read message: %v", err)
			break // Exit the loop if an error occurs
		}
		fmt.Println("Received message:", string(bytes))
	}
}
