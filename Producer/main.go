package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
)

type APIProducer struct {
	apiURL     string
	producer   sarama.SyncProducer
	kafkaTopic string
}

func NewAPIProducer(apiURL, kafkaTopic, kafkaBroker string) (*APIProducer, error) {
	config := sarama.NewConfig()
	producer, err := sarama.NewSyncProducer([]string{kafkaBroker}, config)
	if err != nil {
		return nil, fmt.Errorf("Error creating Kafka producer: %v", err)
	}
	return &APIProducer{
		apiURL:     apiURL,
		producer:   producer,
		kafkaTopic: kafkaTopic,
	}, nil
}

func (p *APIProducer) fetchAndSend() error {
	resp, err := http.Get(p.apiURL)
	if err != nil {
		return fmt.Errorf("Error fetching data from API: %v", err)
	}
	defer resp.Body.Close()

	var data []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return fmt.Errorf("Error decoding API response: %v", err)
	}

	for _, item := range data {
		message, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("Error marshaling message: %v", err)
		}
		kafkaMessage := &sarama.ProducerMessage{
			Topic: p.kafkaTopic,
			Value: sarama.StringEncoder(message),
		}

		partition, offset, err := p.producer.SendMessage(kafkaMessage)
		if err != nil {
			return fmt.Errorf("Error sending message to Kafka: %v", err)
		}
		fmt.Printf("Sent: %v, Partition: %d, Offset: %d\n", item, partition, offset)
		time.Sleep(1 * time.Second) // Simulate delay
	}
	return nil
}

func main() {
	apiURL := "https://jsonplaceholder.typicode.com/todos"
	kafkaTopic := "api_data"
	kafkaBroker := "localhost:9092"

	producer, err := NewAPIProducer(apiURL, kafkaTopic, kafkaBroker)
	if err != nil {
		log.Fatal(err)
	}

	if err := producer.fetchAndSend(); err != nil {
		log.Fatal(err)
	}
}
