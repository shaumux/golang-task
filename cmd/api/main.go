package main

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"

	redis_store "twitch_chat_analysis/internal/app/datastore/redis"
	"twitch_chat_analysis/internal/app/http/handlers"
	"twitch_chat_analysis/internal/app/messagebus/rabbitmq"
)

var (
	rabbitMQConn *amqp.Connection
	redisClient  *redis.Client
	ch           *amqp.Channel
	q            amqp.Queue
)

func main() {
	rabbitMQConn = rabbitmq.InitRabbitMQ()
	defer rabbitMQConn.Close()

	redisClient = redis_store.InitRedis()
	defer redisClient.Close()

	var err error
	//init channel
	ch, err = rabbitMQConn.Channel()
	if err != nil {
		log.Fatalf(err.Error())
	}
	defer ch.Close()

	q, err = ch.QueueDeclare(
		"message",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf(err.Error())
	}

	go rabbitmq.MessageProcessor(ch, q, redisClient)

	if err != nil {
		log.Fatal(err)
	}

	handlersService := handlers.HandlersService{
		RabbitMQConn: rabbitMQConn,
		RedisClient:  redisClient,
		Ch:           ch,
		Q:            q,
	}

	r := gin.Default()

	r.POST("/message", handlersService.Post)

	r.GET("/message/list", handlersService.Get)

	r.GET("/test", func(c *gin.Context) {
		c.JSON(200, "worked")
	})

	r.Run()
}
