package repositories

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"tps-service/structs"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type RedisBankMetricsRepository struct {
	RedisClient *redis.Client
	Ctx context.Context
	lockTokens  sync.Map
}

var unlockScript = redis.NewScript(`
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
`)


var _ BankMetricsRepository = (*RedisBankMetricsRepository)(nil)

func (r *RedisBankMetricsRepository) GetBankMetrics(bankId string) (*structs.BankMetrics, error) {
	fmt.Println("\nFetching Bank Metrics")
	rawData, err := r.RedisClient.HGet(r.Ctx, "bank_metrics", bankId).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("Bank Metric Not Found: %v", err)
		}
	}

	var bankMetrics structs.BankMetrics
	if err := json.Unmarshal([]byte(rawData), &bankMetrics); err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %v", err)
	}
	bankMetrics.TokenAvailable = make(chan struct{}, 1)
	bankMetrics.StopRefillChan = make(chan struct{})
	

	return &bankMetrics, nil
}



func (r *RedisBankMetricsRepository) UpdateBankMetrics(bankId string, bankMetrics *structs.BankMetrics) (string, error) {
	bankMetrics.LastUpdated = time.Now()
	jsonData, err := json.Marshal(&bankMetrics)
	if err != nil {
		fmt.Printf("error marshaling data: %v", err)
		return "", fmt.Errorf("error marshaling data: %v", err)
	}

	
	err = r.RedisClient.HSet(r.Ctx, "bank_metrics", bankId, jsonData).Err()
	if err != nil {
		return "", fmt.Errorf("error saving to Redis: %v", err)
	}

	fmt.Printf("Token Updated: %d\n", bankMetrics.Tokens)
	return string(jsonData), nil
}

// Lock: Sets a key if it's absent (NX) with an expiration time.
func (r *RedisBankMetricsRepository) LockBankMetrics(ctx context.Context, id string) bool {
	token := uuid.NewString()
	success, err := r.RedisClient.SetNX(ctx, id, token, 3*time.Minute).Result()
	if success {
        // Save the token somewhere in memory to use later
		// fmt.Printf("[%s %s] Locked 3\n", id, token)
        r.lockTokens.Store(id, token)
    }
    return success && err == nil
}

// Unlock: Deletes the key if it exists.
func (r *RedisBankMetricsRepository) UnlockBankMetrics(bankId string) bool {
	unlockCtx := context.Background()
	// fmt.Printf("[%s] Unlocking 13\n", bankId)
	value, ok := r.lockTokens.Load(bankId)
    if !ok {
        return false
    }
	// fmt.Printf("[%s] Unlocking 14\n", bankId)

    result, err := unlockScript.Run(unlockCtx, r.RedisClient, []string{bankId}, value).Result()
    if err != nil {
		// fmt.Printf("[%s %s] Failed Unlocking 15 %s\n", bankId, value, err.Error())
        return false
    }
	// fmt.Printf("[%s %s] Unlocked 16\n", bankId, value)
    if deleted, ok := result.(int64); ok && deleted > 0 {
		fmt.Printf("[%s] Unlocked\n", bankId)
        r.RedisClient.Publish(unlockCtx, "unlock_channel", bankId)
        return true
    }
    return false
}

func (r *RedisBankMetricsRepository) WaitForUnlock(ctx context.Context, bankId string) bool {
	pubsub := r.RedisClient.Subscribe(ctx, "unlock_channel")
	defer pubsub.Close()

	for {
		select {
		case <-ctx.Done():
			// fmt.Println("Wait for unlock timed out:", bankId)
			return false

		case msg := <-pubsub.Channel():
			if msg.Payload == bankId {
				// fmt.Println("Received unlock signal for:", bankId)
				if r.LockBankMetrics(ctx, bankId) {
					// fmt.Println("Successfully re-acquired lock:", bankId)
					return true
				}
			}

		default:
			// non-blocking check
			if r.LockBankMetrics(ctx, bankId) {
				// fmt.Println("Successfully acquired lock without signal:", bankId)
				return true
			}
			time.Sleep(50 * time.Millisecond) // small backoff to avoid hot loop
		}
	}
}
