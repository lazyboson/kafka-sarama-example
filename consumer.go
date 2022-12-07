package kf

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
)

type Consumer struct {
	flowEventReader             sarama.ConsumerGroup
	topic                       string
	brokerUrls                  []string
	exampleConsumerGroupHandler sarama.ConsumerGroupHandler
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
	c.exampleConsumerGroupHandler = p{}
	return c
}

type p struct {
	Cons *Consumer
}

func (c *Consumer) HandleMessages() {
	// Consume from kafka and process
	for {
		var err = c.flowEventReader.Consume(context.Background(), []string{c.topic}, &p{Cons: c})
		if err != nil {
			fmt.Println("FAILED")
			continue
		}
		fmt.Println("messages are received")
	}

}
func (p) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (p) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (l p) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("inside the  first ConsumerClaim")
	for msg := range claim.Messages() {
		l.Cons.logMessage(msg)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func (c *Consumer) logMessage(msg *sarama.ConsumerMessage) {
	fmt.Printf("messages: key: %s and val:%s", string(msg.Key), string(msg.Value))
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
