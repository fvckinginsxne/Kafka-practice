package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

const (
	kafkaAddr = "localhost:9092"
	topicName = "test-topic"
	msgNum    = 10
)

func main() {
	admin, err := sarama.NewClusterAdmin(
		[]string{kafkaAddr}, sarama.NewConfig(),
	)
	if err != nil {
		panic(err)
	}
	defer admin.Close()

	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil && err.(*sarama.TopicError).Err != sarama.ErrTopicAlreadyExists {
		panic(err)
	}

	done := make(chan struct{})

	go consumeMessages(done)
	go produceMessages()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	<-sigchan

	log.Print("Shutting down...")

	close(done)
}
