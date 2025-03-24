package main

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"kafka-examples/conf"
	"kafka-examples/utils"
	"log"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Idempotent = true                //生产者开启幂等
	config.Producer.RequiredAcks = sarama.WaitForAll //开启幂等后，所有的isr列表中的broker都ack后，才能算成果
	config.Net.MaxOpenRequests = 1                   //开启幂等后，并发请求数只能为1

	producer, err := sarama.NewSyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatalln("NewSyncProducer failed", err)
	}
	for i := 0; i < 10; i++ {
		str := utils.GenTimeStr()
		msg := &sarama.ProducerMessage{
			Topic: conf.Topic,
			Key:   sarama.StringEncoder(str),
			Value: sarama.StringEncoder(newStr()),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("SendMessage err: ", err)
			return
		}
		log.Printf("[Producer] partitionid: %d; offset:%d, value: %s\n", partition, offset, str)
	}
}

type person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func newStr() string {
	p := person{
		Name: "yst",
		Age:  10,
	}
	marshalStr, err := json.Marshal(p)
	if err != nil {
		log.Fatalln("json.Marshal failed", err)
	}
	return string(marshalStr)
}
