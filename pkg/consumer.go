package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

type Consumer struct {
	flowEventReader sarama.ConsumerGroup
	topic           string
	brokerUrls      []string
}

type data struct {
	Name     string `json:"name"`
	Employee string `json:"employee"`
}

func InitConsumer(brokers []string, topic string) *Consumer {
	c := &Consumer{}
	c.topic = topic
	c.brokerUrls = brokers
	var (
		err error
	)
	conf := createSaramaKafkaConf()
	c.flowEventReader, err = sarama.NewConsumerGroup(c.brokerUrls, "myconf", conf)
	if err != nil {
		panic("failed to create consumer group on kafka cluster")
	}

	return c
}

type KafkaConsumerGroupHandler struct {
	Cons *Consumer
}

func (c *Consumer) HandleMessages() {
	// Consume from kafka and process
	for {
		var err = c.flowEventReader.Consume(context.Background(), []string{c.topic}, &KafkaConsumerGroupHandler{Cons: c})
		if err != nil {
			fmt.Println("FAILED")
			continue
		}
	}

}
func (*KafkaConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*KafkaConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (l *KafkaConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		l.Cons.logMessage(msg)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func (c *Consumer) logMessage(msg *sarama.ConsumerMessage) {
	d := &data{}
	err := json.Unmarshal(msg.Value, d)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("time: %s messages: key: %s and val:%+v", time.Now().String(), string(msg.Key), d)
	fmt.Println()
}

func createSaramaKafkaConf() *sarama.Config {
	conf := sarama.NewConfig()
	version := "2.6.2"
	kafkaVer, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		panic("failed to parse kafka version, executor will not run")
	}
	conf.Version = kafkaVer
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}

	return conf
}
