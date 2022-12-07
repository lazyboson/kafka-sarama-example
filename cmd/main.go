package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-uuid"
	kf "kafka-Sarama"
	"time"
)

type data struct {
	Name     string `json:"name"`
	Employee string `json:"employee"`
}

func main() {
	brokers := []string{"localhost:9092"}
	topic := "kafka-sarama-poc"
	prod := kf.InitProducer(brokers, topic)
	for i := 0; i < 10; i++ {
		key, _ := uuid.GenerateUUID()
		val := &data{
			Name:     "ashutosh",
			Employee: "self-employed",
		}
		d, _ := json.Marshal(val)
		err := prod.WriteMessage(key, d)
		if err != nil {
			fmt.Print("error in data production")
		}
	}
	cons := kf.InitConsumer(brokers, topic)
	go cons.HandleMessages()
	time.Sleep(2 * time.Second)
}
