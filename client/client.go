package main

import (
	"context"
	"fmt"
	"log"
	"time"

	protos "github.com/Mensurui/grpc-kafka/kafka-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Create a context with a timeout to control the dialing duration
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Dial the server with context using insecure credentials (no TLS)
	conn, err := grpc.DialContext(ctx, "localhost:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Create a new KafkaService client
	client := protos.NewKafkaServiceClient(conn)

	// Set up a context with a timeout for the HealthCheck request
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Call the HealthCheck method
	for i := 0; i < 5; i++ {
		response, err := client.HealthCheck(ctx, &protos.HealthCheckRequest{})
		if err != nil {
			log.Fatalf("Error calling HealthCheck: %v", err)
		}

		// Print the response from the HealthCheck call
		fmt.Printf("HealthCheck Response: %s, Status: %v\n", response.Message, response.Status)
	}
}
