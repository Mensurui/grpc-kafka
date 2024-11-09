package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "orders"
	worker, err := connectConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	fmt.Println("consumer started")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0
	doneChan := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Printf("Error: %v", err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Recieved message count: %d| Topic: %s| Message: %s\n", msgCount, topic, string(msg.Value))
			case <-sigchan:
				fmt.Println("Interupption detected")
				doneChan <- struct{}{}
				return
			}
		}
	}()

	<-doneChan
	fmt.Println("Consumer stopped")

}

func connectConsumer(brokerURL []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = false

	conn, err := sarama.NewConsumer(brokerURL, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
