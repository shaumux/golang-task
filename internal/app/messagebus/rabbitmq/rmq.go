package rabbitmq

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"

	"twitch_chat_analysis/config"
	"twitch_chat_analysis/internal/app/models"
)

func InitRabbitMQ() *amqp.Connection {
	conn, err := amqp.Dial(config.RabbitMQURI)
	if err != nil {
		log.Fatal(err)
	}

	return conn
}

func MessageProcessor(ch *amqp.Channel, q amqp.Queue, redisClient *redis.Client) {
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to get a consumer: %v", err)
	}
	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)

			var message models.Message
			err := json.Unmarshal(d.Body, &message)
			if err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				continue
			}

			key := fmt.Sprintf("%s:%s", message.Sender, message.Receiver)
			jsonMessage, err := json.Marshal(message)
			if err != nil {
				log.Printf("Failed to marshal message: %v", err)
				continue
			}
			err = redisClient.RPush(redisClient.Context(), key, jsonMessage).Err()
			if err != nil {
				log.Printf("Failed to save message to Redis: %v", err)
				continue
			}

			log.Printf("Processed message: %s", d.Body)
		}
	}()

	log.Printf("Waiting for messages...")
	<-forever
}
