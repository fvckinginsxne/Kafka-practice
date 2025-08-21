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

	wg.Add(numWorkers)
	for i := range numWorkers {
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
		close(msgChan)
	}()

	for {
		select {
		case <-done:
			wg.Wait()
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
			log.Printf("Worker %d processing message [%s] (offset: %d), (partition: %d), (key: %s)",
				id, string(msg.Value), msg.Offset, msg.Partition, string(msg.Key))
		case <-done:
			log.Printf("Worker %d shutting down", id)
			return
		}
	}
}
