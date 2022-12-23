package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-uuid"
	"kafka-Sarama/pkg"
	"time"
)

type data struct {
	Name     string `json:"name"`
	Employee string `json:"employee"`
}

func main() {
	brokers := []string{"local-kafka-cp-kafka.voice.svc.cluster.local:9092"}
	topic := "kafka-sarama-poc"
	prod := pkg.InitProducer(brokers, topic)
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
	cons := pkg.InitConsumer(brokers, topic)
	cons.HandleMessages()
	time.Sleep(2 * time.Second)
}
