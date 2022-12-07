package kf

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
)

type Consumer struct {
	flowEventReader sarama.ConsumerGroup
	topic           string
	brokerUrls      []string
}

func InitConsumer(brokers []string, topic string) *Consumer {
	c := &Consumer{}
	c.topic = topic
	c.brokerUrls = brokers
	var (
		err error
	)
	conf := createSaramaKafkaConf()
	c.flowEventReader, err = sarama.NewConsumerGroup(c.brokerUrls, "flowExecutor", conf)
	if err != nil {
		panic("failed to create consumer group on kafka cluster")
	}
	return c
}

func (c *Consumer) HandleMessages() {
	// Consume from kafka and process
	for {
		err := c.flowEventReader.Consume(context.Background(), []string{c.topic}, exampleConsumerGroupHandler{})
		if err != nil {
			fmt.Errorf("FAILED")
		}
	}
}

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
	}
	return nil
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
	conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky

	return conf
}
