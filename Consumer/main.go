package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	kafkaBroker := "localhost:9092" // Kafka broker address
	topic := "api_data"             // Topic to consume from
	group := "consumer-group-1"     // Consumer group name

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create a new consumer group
	consumerGroup, err := sarama.NewConsumerGroup([]string{kafkaBroker}, group, config)
	if err != nil {
		log.Fatal("Error creating consumer group:", err)
	}
	defer consumerGroup.Close()

	// Implement a consumer handler
	handler := &ConsumerHandler{}

	// Start consuming messages
	for {
		err := consumerGroup.Consume(nil, []string{topic}, handler)
		if err != nil {
			log.Println("Error in consumer:", err)
		}
	}
}

// ConsumerHandler handles incoming Kafka messages
type ConsumerHandler struct{}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim processes Kafka messages
func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Printf("Message received: %s\n", string(message.Value))
		session.MarkMessage(message, "")
	}
	return nil
}
