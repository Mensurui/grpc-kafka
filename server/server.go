package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/IBM/sarama"
	protos "github.com/Mensurui/grpc-kafka/kafka-go/proto"
	"github.com/hashicorp/go-hclog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Order struct {
	ID       string `form:"id" json:"id"`
	ItemName string `form:"item_name" json:"item_name"`
}

func main() {
	l := hclog.Default()
	ss := NewServer(l)
	gs := grpc.NewServer()
	protos.RegisterKafkaServiceServer(gs, ss)
	reflection.Register(gs)

	listen, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("Failed to listen on port 9090: %v", err)
	}

	// Start the server in a goroutine for graceful shutdown
	go func() {
		if err := gs.Serve(listen); err != nil {
			log.Fatalf("Failed to serve gRPC server: %v", err)
		}
	}()
	l.Info("Serving on port :9090")
	fmt.Println("Serving on port :9090")

	// Wait indefinitely or handle graceful shutdown
	select {}
}

func ConnectProducer(brokerURL []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokerURL, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func PushOrderToQueue(topic string, message []byte) error {
	brokerURL := []string{"localhost:9092"}
	producer, err := ConnectProducer(brokerURL)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka producer: %w", err)
	}
	defer producer.Close()

	msg := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	partition, offset, err := producer.SendMessage(&msg)
	if err != nil {
		return fmt.Errorf("failed to send message to Kafka: %w", err)
	}

	fmt.Printf("Message stored in the topic: %s/ partition: %d/ offset: %d\n", topic, partition, offset)
	return nil
}

type Server struct {
	protos.UnimplementedKafkaServiceServer
	l hclog.Logger
}

func NewServer(l hclog.Logger) *Server {
	return &Server{
		l: l,
	}
}

func (s *Server) HealthCheck(ctx context.Context, in *protos.HealthCheckRequest) (*protos.HealthCheckResponse, error) {
	return &protos.HealthCheckResponse{
		Message: "Healthy",
		Status:  true,
	}, nil
}

func (s *Server) Order(ctx context.Context, in *protos.OrderRequest) (*protos.OrderResponse, error) {
	// Extract order data
	id := in.Id
	item := in.ItemName
	order := Order{
		ID:       id,
		ItemName: item,
	}

	// Convert order struct to JSON byte slice
	orderBytes, err := json.Marshal(order)
	if err != nil {
		s.l.Error("Error marshaling order", "order", order, "error", err)
		return nil, fmt.Errorf("failed to marshal order: %w", err)
	}

	// Push order to Kafka queue
	if err := PushOrderToQueue("orders", orderBytes); err != nil {
		s.l.Error("Failed to push order to queue", "order", order, "error", err)
		return nil, fmt.Errorf("failed to push order to Kafka: %w", err)
	}

	// Return successful response
	return &protos.OrderResponse{
		Message: fmt.Sprintf("Successfully ordered %s: %s", id, item),
		Success: true,
	}, nil
}

