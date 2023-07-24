package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/wagslane/go-rabbitmq"
	"log"
	payload2 "twitch_chat_analysis/payload"
)

func main() {
	conn, err := rabbitmq.NewConn(
		"amqp://user:password@localhost:7001",
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
	})
	defer rdb.Close()
	consumer, err := rabbitmq.NewConsumer(
		conn,
		func(d rabbitmq.Delivery) rabbitmq.Action {
			var data payload2.Payload
			err := json.Unmarshal(d.Body, &data)
			if err != nil {
				log.Println(err)
				return rabbitmq.NackDiscard
			}
			log.Printf("New Delivery from %s to %s", data.Sender, data.Receiver)
			redisKey := fmt.Sprintf("%s+%s", data.Sender, data.Receiver)
			rawList, err := rdb.Get(context.Background(), redisKey).Bytes()
			if err != nil && err != redis.Nil {
				log.Println(err)
				return rabbitmq.NackDiscard
			}
			var list []payload2.Payload
			if err != redis.Nil {
				err = json.Unmarshal(rawList, &list)
				if err != nil {
					log.Println(err)
					return rabbitmq.NackDiscard
				}
			} else {
				list = make([]payload2.Payload, 0)
			}
			list = append([]payload2.Payload{data}, list...)
			rawList, err = json.Marshal(&list)
			if err != nil {
				log.Println(err)
				return rabbitmq.NackDiscard
			}
			err = rdb.Set(context.Background(), redisKey, rawList, redis.KeepTTL).Err()
			if err != nil {
				log.Println(err)
				return rabbitmq.NackDiscard
			}
			return rabbitmq.Ack
		},
		"my_queue",
		rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key"),
		rabbitmq.WithConsumerOptionsExchangeName("events"),
		rabbitmq.WithConsumerOptionsExchangeDeclare,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for {
	}
}
