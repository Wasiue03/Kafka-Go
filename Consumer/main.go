package main

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type consumer struct{}

func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (consumer *consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Printf("Message received: %s\n", string(message.Value))
		sess.MarkMessage(message, "")
	}
	return nil
}

func main() {
	brokers := []string{"localhost:9092"}
	group := "example-group"
	topics := []string{"api_data"}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Version = sarama.V2_8_0_0

	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx := context.Background()
	for {
		err := consumerGroup.Consume(ctx, topics, &consumer{})
		if err != nil {
			log.Printf("Error in consumer group: %v", err)
		}
	}
}
