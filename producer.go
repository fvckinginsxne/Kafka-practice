package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func produceMessages() {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(
		[]string{kafkaAddr}, config,
	)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case success, ok := <-producer.Successes():
				if !ok {
					return
				}
				msgContent := success.Value.(sarama.StringEncoder)
				log.Printf("Send %s to partition %d at offset %d\n",
					string(msgContent), success.Partition, success.Offset)
			case err, ok := <-producer.Errors():
				if !ok {
					return
				}
				log.Printf("Failed to send message: %v\n", err)
			case <-done:
				return
			}
		}
	}()

	for i := range msgNum {
		msg := &sarama.ProducerMessage{
			Topic: topicName,
			Value: sarama.StringEncoder(fmt.Sprintf("message: %d", i)),
		}

		producer.Input() <- msg

		time.Sleep(500 * time.Millisecond)
	}

	log.Print("Finished producing messages")
}
