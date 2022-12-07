package kf

import (
	"github.com/Shopify/sarama"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

type Producer struct {
	flowEventProducer sarama.SyncProducer
	topic             string
}

func InitProducer(brokers []string, topic string) *Producer {
	CreateKafkaTopic(brokers[0], topic)
	p := &Producer{}
	prod, err := newFlowWriter(brokers)
	if err != nil {
		panic("failed to connect to producer")
	}
	p.flowEventProducer = prod
	p.topic = "test-poc-topic"
	return p
}

func CreateKafkaTopic(kafkaURL, topic string) {
	conn, err := kafka.Dial("tcp", kafkaURL)
	if err != nil {
		panic(err.Error())
	}
	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}

	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}

	defer controllerConn.Close()
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 2,
		},
	}
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()
}

func newFlowWriter(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	version := "2.6.2"
	kafkaVer, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		panic("failed to parse kafka version, producer will not run")
	}
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = kafkaVer
	producer, err := sarama.NewSyncProducer(brokers, config)

	return producer, err
}

func (p *Producer) WriteMessage(uuid, data string) error {
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.ByteEncoder(uuid),
		Value: sarama.ByteEncoder(data),
	}

	_, _, err := p.flowEventProducer.SendMessage(msg)
	if err != nil {
		return err
	}
	return nil
}
