package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"log"
	"net/http"
	payload2 "twitch_chat_analysis/payload"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
	})
	defer rdb.Close()

	r := gin.Default()

	r.GET("/message/list/:sender/:receiver", func(context *gin.Context) {
		sender := context.Param("sender")
		receiver := context.Param("receiver")
		rawData, err := rdb.Get(context, fmt.Sprintf("%s+%s", sender, receiver)).Bytes()
		if err != nil {
			log.Println(err)
			context.AbortWithStatus(http.StatusBadRequest)
			return
		}
		var data []payload2.Payload
		err = json.Unmarshal(rawData, &data)
		if err != nil {
			log.Println(err)
			context.AbortWithStatus(http.StatusBadRequest)
			return
		}
		context.JSON(http.StatusOK, data)
	})

	err := r.Run(":8081")
	if err != nil {
		log.Fatal(err)
	}
}
