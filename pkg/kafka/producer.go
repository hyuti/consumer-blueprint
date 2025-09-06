package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	prod     *kafka.Producer
	topicMap map[string]string
}

// Produce for using this function you must register a topic beforehand with RegisterTopic. Otherwise, you can use ProduceBytes instead
func (s *Producer) Produce(msg MsgNameType) error {
	topic, ok := s.topicMap[msg.Name()]
	if !ok {
		return fmt.Errorf("unable to find topic for %v msg", msg.Name())
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return s.ProduceBytes(b, topic)
}

func (s *Producer) ProduceBytes(b []byte, topic string) error {
	s.prod.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: b,
	}
	return nil
}

// RegisterTopic f
func (s *Producer) RegisterTopic(key, topic string) {
	s.topicMap[key] = topic
}

func (s *Producer) Close() {
	s.prod.Close()
}

func NewProducer(broker string, opts ...func(*kafka.ConfigMap) error) (*Producer, error) {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers": broker,
		"linger.ms":         5,
	}
	for _, opt := range opts {
		err := opt(cfg)
		if err != nil {
			return nil, err
		}
	}
	p, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}
	return &Producer{
		prod:     p,
		topicMap: map[string]string{},
	}, nil
}
