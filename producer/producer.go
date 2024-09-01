package main

import (
	"context"
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

	// Set a write deadline to prevent indefinite waits
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))

	// Write a message to the Kafka topic
	_, err = conn.WriteMessages(kafka.Message{Value: []byte("Hi, I am a new developer")})
	if err != nil {
		log.Fatalf("failed to write messages: %v", err)
	}

	log.Println("Message sent successfully!")
}
