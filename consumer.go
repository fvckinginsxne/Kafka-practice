package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func consumeMessages(done <-chan struct{}) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{kafkaAddr},
		"my-consumer-group",
		config,
	)
	if err != nil {
		panic(err)
	}
	defer consumerGroup.Close()

	numWorkers := 3
	wg := &sync.WaitGroup{}
	msgChan := make(chan *sarama.ConsumerMessage, numWorkers*2)
	workerDone := make(chan struct{})

	for i := range numWorkers {
		wg.Add(1)
		go worker(i, msgChan, done, wg)
	}

	handler := consumerGroupHandler{
		messages: msgChan,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-done
		cancel()
		close(workerDone)
	}()

	for {
		select {
		case <-done:
			return
		default:
			err := consumerGroup.Consume(ctx, []string{topicName}, &handler)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					return
				}
				log.Printf("Error from consumer: %v", err)
				time.Sleep(1 * time.Second)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}
}

type consumerGroupHandler struct {
	messages chan<- *sarama.ConsumerMessage
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		h.messages <- msg
		session.MarkMessage(msg, "")
	}
	return nil
}

func worker(
	id int,
	messages <-chan *sarama.ConsumerMessage,
	done <-chan struct{},
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		select {
		case msg := <-messages:
			log.Printf("Worker %d processing message [%s] (offset: %d), (partition: %d)",
				id, string(msg.Value), msg.Offset, msg.Partition)
		case <-done:
			log.Printf("Worker %d shutting down", id)
			return
		}
	}
}
