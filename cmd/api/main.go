package main

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/wagslane/go-rabbitmq"
	"log"
	"net/http"
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

	publisher, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName("events"),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	r := gin.Default()

	r.POST("/message", func(context *gin.Context) {
		rawData, err := context.GetRawData()
		if err != nil {
			log.Println(err)
			context.AbortWithStatus(http.StatusBadRequest)
			return
		}
		var payload payload2.Payload
		err = json.Unmarshal(rawData, &payload)
		if err != nil {
			log.Println(err)
			context.AbortWithStatus(http.StatusBadRequest)
			return
		}
		err = publisher.Publish(rawData, []string{"my_routing_key"},
			rabbitmq.WithPublishOptionsContentType("application/json"),
			rabbitmq.WithPublishOptionsExchange("events"))
		if err != nil {
			log.Println(err)
		}
		context.Status(http.StatusOK)
	})

	err = r.Run()
	if err != nil {
		log.Fatal(err)
	}
}
