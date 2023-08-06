package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"twitch_chat_analysis/internal/app/models"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"
)

type HandlersService struct {
	RabbitMQConn *amqp.Connection
	RedisClient  *redis.Client
	Ch           *amqp.Channel
	Q            amqp.Queue
}

func (h *HandlersService) Post(c *gin.Context) {
	var message models.Message

	err := c.BindJSON(&message)
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	err = PublishMessage(message, h.RabbitMQConn, h.Ch, h.Q)
	if err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.Status(http.StatusOK)
}

func (h *HandlersService) Get(c *gin.Context) {
	sender := c.Query("sender")
	receiver := c.Query("receiver")

	messages, err := GetMessages(sender, receiver, h.RedisClient)
	if err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	result := make([]models.Message, len(messages))
	for i, message := range messages {
		result[len(messages)-1-i] = message
	}

	c.JSON(http.StatusOK, result)
}

func PublishMessage(message models.Message, rabbitMQConn *amqp.Connection, ch *amqp.Channel, q amqp.Queue) error {
	var err error
	ch, err = rabbitMQConn.Channel()
	if err != nil {
		return err
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
		return err
	}

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		return err
	}

	err = ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonMessage,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func GetMessages(sender string, receiver string, redisClient *redis.Client) ([]models.Message, error) {
	var messages []models.Message
	if sender == "" && receiver == "" {
		keys, err := redisClient.Keys(redisClient.Context(), "*").Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			jsonMessage, err := redisClient.Get(redisClient.Context(), key).Bytes()
			if err != nil {
				return nil, err
			}

			var message models.Message
			err = json.Unmarshal(jsonMessage, &message)
			if err != nil {
				return nil, err
			}

			messages = append(messages, message)
		}
	} else {
		key := fmt.Sprintf("%s:%s", sender, receiver)
		jsonMessages, err := redisClient.LRange(redisClient.Context(), key, 0, -1).Result()
		if err != nil {
			return nil, err
		}

		for _, jsonMessage := range jsonMessages {
			var message models.Message
			err = json.Unmarshal([]byte(jsonMessage), &message)
			if err != nil {
				return nil, err
			}

			messages = append(messages, message)
		}
	}

	return messages, nil
}
