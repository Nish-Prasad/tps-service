package service

import (
	"context"
	"fmt"
	"sync"

	"tps-service/repositories"
	"tps-service/structs"
	"github.com/go-redis/redis/v8"
)

var redisClient *redis.Client

type BankUpdateRequest struct {
	bankMetrics *structs.BankMetrics
	success     bool
}

type BankStatsQueue struct {
	bankMetricsRepository repositories.BankMetricsRepository
	updateQueue           chan BankUpdateRequest
	wg                    sync.WaitGroup
}

func NewRedisBankMetricsRepository(client *redis.Client) *repositories.RedisBankMetricsRepository {
	if client != nil && redisClient == nil {
		redisClient = client
	}
	return &repositories.RedisBankMetricsRepository{
		RedisClient: client,
		Ctx:         context.Background(),
	}
}

func NewBankStatsQueue() *BankStatsQueue {
	queue := &BankStatsQueue{
		bankMetricsRepository: NewRedisBankMetricsRepository(redisClient),
		updateQueue:           make(chan BankUpdateRequest, 100),
	}
	queue.startProcessingQueue()
	return queue
}

func (b *BankStatsQueue) SendToQueue(bankMetrics *structs.BankMetrics, success bool) {
	b.updateQueue <- BankUpdateRequest{bankMetrics: bankMetrics, success: success}
}

func (b *BankStatsQueue) startProcessingQueue() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for updateRequest := range b.updateQueue {
			b.processBankMetrics(updateRequest)
		}
	}()
}

func (b *BankStatsQueue) processBankMetrics(updateRequest BankUpdateRequest) {
	bankMetrics := updateRequest.bankMetrics
	if bankMetrics != nil {
		if updateRequest.success {
			bankMetrics.UpdateSuccess()
		} else {
			bankMetrics.UpdateFailed()
		}
		fmt.Print("Updating Bank Metric in Redis: " + bankMetrics.BankId)
		b.bankMetricsRepository.UpdateBankMetrics(bankMetrics.BankId, bankMetrics)
		fmt.Println("Updated Bank Metrics:", bankMetrics.BankId)
	}
}

func (b *BankStatsQueue) CloseQueue() {
	close(b.updateQueue)
	b.wg.Wait()
}
