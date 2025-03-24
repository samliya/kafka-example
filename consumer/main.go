package main

import (
	"context"
	"fmt"
	"kafka-examples/conf"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

func main() {
	topic := conf.Topic
	// 使用示例的消费者组名称和消费者名称
	ConsumerGroup(topic, "my-group", "consumer-1")
}

/*单分区消费者的实现*/
func singlePartition(topic string) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatalln("failed to new consumer", err)
	}
	defer consumer.Close()
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalln("consumer.ConsumePartition failed", err)
	}

	for message := range partitionConsumer.Messages() {
		log.Printf("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, string(message.Value))
	}
}

/*多分区消费者的实现*/
func MutiPartitions(topic string) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewConsumer err: ", err)
	}
	defer consumer.Close()
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatal("Partitions err: ", err)
	}
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for _, patitionId := range partitions {
		go consumerPartition(consumer, topic, patitionId, &wg)
	}
	wg.Wait()
}

func consumerPartition(consumer sarama.Consumer, topic string, partitionId int32, wg *sync.WaitGroup) {
	defer wg.Done()
	partitionConsumer, _ := consumer.ConsumePartition(topic, partitionId, sarama.OffsetOldest)
	defer partitionConsumer.Close()
	for message := range partitionConsumer.Messages() {
		log.Printf("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, string(message.Value))
	}
}

func ConsumerGroup(topic, group, name string) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	consumerGroup, err := sarama.NewConsumerGroup([]string{conf.HOST}, group, config)
	if err != nil {
		log.Fatalln("NewConsumerGroup failed,err", err)
	}

	defer consumerGroup.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for err = range consumerGroup.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	go func() {
		defer wg.Done()
		handler := MYConsumerGroupHandler{name: name}
		for {
			fmt.Println("running", name)
			if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
				log.Println("Consume err: ", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	wg.Wait()
}

/*消费者组的实现*/

type MYConsumerGroupHandler struct {
	name  string
	count int64
}

func (MYConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (MYConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h MYConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			fmt.Printf("[consumer] name:%s topic:%q partition:%d offset:%d\n", h.name, msg.Topic, msg.Partition, msg.Offset)
			// 标记消息已被消费 内部会更新 consumer offset
			session.MarkMessage(msg, "")
			h.count++
			if h.count%10000 == 0 {
				fmt.Printf("name:%s 消费数:%v\n", h.name, h.count)
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
