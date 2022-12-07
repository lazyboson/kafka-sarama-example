package main

import (
	"fmt"
	"github.com/hashicorp/go-uuid"
	kf "kafka-Sarama"
	"time"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "test-poc-topic"
	prod := kf.InitProducer(brokers, topic)
	for i := 0; i < 100; i++ {
		key, _ := uuid.GenerateUUID()
		err := prod.WriteMessage(key, string(i))
		if err != nil {
			fmt.Print("error in data production")
		}
	}
	cons := kf.InitConsumer(brokers, topic)
	go cons.HandleMessages()
	time.Sleep(2 * time.Second)
}
