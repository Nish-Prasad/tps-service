package config

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

var RedisClient *redis.Client

var Ctx = context.Background()

func InitialiseRedis() {
	ctx := context.Background()

	RedisClient = redis.NewClient(&redis.Options{
		Addr:     "redis-19387.c114.us-east-1-4.ec2.redns.redis-cloud.com:19387",
		Username: "default",
		Password: "O2k27csFEQIz7llh46Byq8XAnuYrFmA6",
		DB:       0,
	})

	RedisClient.Set(ctx, "foo", "bar", 0)
	result, err := RedisClient.Get(ctx, "foo").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(result) // >>> bar
}

func CloseRedis() {
    if RedisClient != nil {
        err := RedisClient.Close()
        if err != nil {
            fmt.Println("Error closing Redis:", err)
        } else {
            fmt.Println("Redis connection closed.")
        }
    }
}

func KillIdleClients() {
    ctx := context.Background()

    clients, err := RedisClient.ClientList(ctx).Result()
    if err != nil {
        fmt.Println("Error fetching client list:", err)
        return
    }

    for _, line := range strings.Split(clients, "\n") {
        if strings.TrimSpace(line) == "" {
            continue
        }

        // Parse client info
        fields := strings.Split(line, " ")
        var id string
        var idleTime int
        for _, field := range fields {
            parts := strings.Split(field, "=")
            if len(parts) != 2 {
                continue
            }
            switch parts[0] {
            case "id":
                id = parts[1]
            case "idle":
                idleTime, _ = strconv.Atoi(parts[1])
            }
        }

        // If idle more than 300 seconds, kill it
        if idleTime > 300 {
            fmt.Println("Killing idle client:", id)
            err := RedisClient.Do(ctx, "CLIENT", "KILL", "ID", id).Err()
            if err != nil {
                fmt.Println("Failed to kill client:", err)
            }
        }
    }
}