package redis_store

import (
	"log"

	"github.com/go-redis/redis/v8"

	"twitch_chat_analysis/config"
)

func InitRedis() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: config.RedisURI,
	})

	_, err := client.Ping(client.Context()).Result()
	if err != nil {
		log.Fatal(err)
	}

	return client
}
