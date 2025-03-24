package main

import (
	"context"
	"github.com/IBM/sarama"
	"kafka-examples/conf"
	"kafka-examples/utils"
	"log"
	"sync"
	"time"
)

/*同步生产*/
func syncProducer(topic string, limit int) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err := sarama.NewSyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatalln("NewSyncProducer failed err", err)
	}
	defer producer.Close()

	var successes, errors int
	for i := 0; i < limit; i++ {
		str := utils.GenTimeStr()
		message := &sarama.ProducerMessage{
			Topic:     topic,
			Key:       nil,
			Value:     sarama.StringEncoder(str),
			Headers:   nil,
			Metadata:  nil,
			Offset:    0,
			Partition: 0,
			Timestamp: time.Time{},
		}
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Printf("SendMessage:%d err:%v\n ", i, err)
			errors++
			continue
		}
		successes++
		log.Printf("[Producer] partitionid: %d; offset:%d, value: %s\n", partition, offset, str)
	}
	log.Printf("发送完毕 总发送条数:%d successes: %d errors: %d\n", limit, successes, errors)
}

/*异步生产*/
func asyncProducer(topic string, limit int) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewSyncProducer err:", err)
	}
	var (
		wg                                   sync.WaitGroup
		enqueued, timeout, successes, errors int
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Errors() {
			errors++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	for i := 0; i < limit; i++ {
		str := utils.GenTimeStr()
		message := &sarama.ProducerMessage{
			Topic:     topic,
			Key:       nil,
			Value:     sarama.StringEncoder(str),
			Headers:   nil,
			Metadata:  nil,
			Offset:    0,
			Partition: 0,
			Timestamp: time.Time{},
		}
		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
		select {
		case producer.Input() <- message:
			enqueued++
		case <-ctx.Done():
			timeout++
		}
		cancelFunc()
		if i%500 == 0 && i != 0 {
			log.Printf("已发送消息数:%d 超时数:%d\n", i, timeout)
		}
	}
	producer.AsyncClose()
	wg.Wait()
	log.Printf("发送完毕 总发送条数:%d enqueued:%d timeout:%d successes: %d errors: %d\n", limit, enqueued, timeout, successes, errors)
}
