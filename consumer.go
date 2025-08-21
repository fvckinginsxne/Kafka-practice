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
	config.Consumer.Offsets.AutoCommit.Enable = false

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
		done: done,
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
	done <-chan struct{}
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session started")
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group sessetion ending")
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	batchSize := 3
	processedCount := 0

	for msg := range claim.Messages() {
		select {
		case <-h.done:
			if processedCount > 0 {
				session.Commit()
				log.Printf("Partition %d: committed %d messages before shutdown", 
					claim.Partition(), processedCount)
			}
			return nil
		default:
			select {
			case h.messages <- msg:
				session.MarkMessage(msg, "")
				processedCount++
	
				if processedCount >= batchSize {
					session.Commit()
					log.Printf("Partition %d: commited batch of %d messages", 
						claim.Partition(), processedCount)
					processedCount = 0
				}
			case <-h.done:
				if processedCount > 0 {
					session.Commit()
					log.Printf("Partition %d: committed %d messages before shutdown", 
						claim.Partition(), processedCount)
				}
				return nil
			}
		}
	}
	
	if processedCount > 0 {
		session.Commit()
		log.Printf("Partition %d: committed %d final messages", 
			claim.Partition(), processedCount)
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
