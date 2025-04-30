package main

import (
	"container/list"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync/atomic"
	"syscall"

	"sync"
	"time"

	rediesConfig "tps-service/config"
	"tps-service/service"
	"tps-service/structs"
)

func init() {
	service.InitializeLogger()
	rediesConfig.InitialiseRedis()
}

func main() {
	// rediesConfig.KillIdleClients()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		fmt.Println("Shutting down...")
		rediesConfig.CloseRedis()
		os.Exit(0)
	}()
	// testTokenBucketSingleInstance()
	testForMultipleInstance()
}

func testTokenBucketSingleInstance() {
	redisRepo := service.NewRedisBankMetricsRepository(rediesConfig.RedisClient)

	// Create BankMetrics instance
	bankIds := []string{"bank1", "bank2"}
	LastUpdated := time.Now()
	fmt.Println("Last Updated:", LastUpdated)
	allBanksMap := make(map[string]*structs.Banks) 
	allBanks := &[]structs.Banks{}
	bankList := list.New()

	tps := service.GetTpsService()
	for _, bankId := range bankIds {
		bm := structs.NewBankMetrics(bankId)
		structs.GlobalBankMetricMap[bankId] = bm
		bank := structs.Banks{BankID: bankId}
		allBanksMap[bankId] = &bank
		*allBanks = append(*allBanks, bank)
		bankList.PushBack(&bank)
		redisRepo.UpdateBankMetrics(bankId, bm)
		fmt.Println(bm)
		service.StartRefillTicker(bankId, tps)
	}

	// Start the refill ticker for the bank

	// Run loop for 10 iterations
	for i := 0; i < 30; i++ {
		reqId := generateRequestId()
		// Try consuming tokens
		bm := service.TryConsumeToken(allBanksMap, bankList, reqId, tps)
				if bm != nil {
					service.Log(reqId, fmt.Sprintf("Token consumed for %s, iteration %d\n\n", bm.BankId, i+1))
					fmt.Printf("Token consumed for %s, iteration %d\n", bm.BankId, i+1)
				} else {
					service.Log(reqId, fmt.Sprintf("Failed to consume token, iteration %d\n", i+1))
					fmt.Printf("Failed to consume token for, iteration %d\n", i+1)
				}

		// Wait for a few cycles before testing again
		time.Sleep(1 * time.Second) // Adjust time as necessary for testing
	}

	// Stop the refill ticker for the bank
	for _, bankId := range bankIds {
		service.StopRefillTicker(bankId)
	}
}

func testForMultipleInstance() {
	redisRepo := service.NewRedisBankMetricsRepository(rediesConfig.RedisClient)
	// Create BankMetrics instance
	bankIds := []string{"bank1", "bank2"}
	LastUpdated := time.Now()
	fmt.Println("Last Updated:", LastUpdated)

	allBanksMap := make(map[string]*structs.Banks) 
	allBanks := &[]structs.Banks{}
	bankList := list.New()

	tps := service.GetTpsService()
	for _, bankId := range bankIds {
		bm := structs.NewBankMetrics(bankId)
		structs.GlobalBankMetricMap[bankId] = bm
		bank := structs.Banks{BankID: bankId}
		allBanksMap[bankId] = &bank
		*allBanks = append(*allBanks, bank)
		bankList.PushBack(&bank)
		redisRepo.UpdateBankMetrics(bankId, bm)
		fmt.Println(bm)
		service.StartRefillTicker(bankId, tps)
	}

	// Number of concurrent instances
	numInstances := 3
	var wg sync.WaitGroup

	// Simulate multiple instances consuming tokens
	for i := 1; i <= numInstances; i++ {
		wg.Add(1)
		go func(instanceId int) {
			defer wg.Done()
			for j := 0; j < 10; j++ { // Each instance tries 20 times
				reqId := generateRequestId()
				bm := service.TryConsumeToken(allBanksMap, bankList, reqId, tps)
				if bm != nil {
					service.Log(reqId, fmt.Sprintf("[Instance %d] Token consumed for %s, attempt %d\n", instanceId, bm.BankId, j+1))
					fmt.Printf("[Instance %d] Token consumed for %s, attempt %d\n", instanceId, bm.BankId, j)
				} else {
					service.Log(reqId, fmt.Sprintf("[Instance %d] Failed to consume token for any bank, attempt %d\n", instanceId, j+1))
					fmt.Printf("[Instance %d] Failed to consume token for any bank, attempt %d\n", instanceId, j)
				}
				time.Sleep(1 * time.Second) // Small delay
			}
		}(i)
	}

	// Wait for all Goroutines to finish
	wg.Wait()

	// Stop the refill ticker
	for _, bankId := range bankIds {
		service.StopRefillTicker(bankId)
	}

	grouped := service.GroupedLogs()

	// First, collect all request IDs into a slice
	var reqIDs []string
	for reqID := range grouped {
		reqIDs = append(reqIDs, reqID)
	}

	// Sort the request IDs
	sort.Slice(reqIDs, func(i, j int) bool {
		// Convert to int to sort numerically
		id1, _ := strconv.Atoi(reqIDs[i])
		id2, _ := strconv.Atoi(reqIDs[j])
		return id1 < id2
	})

	// Now, print in sorted order
	for _, reqID := range reqIDs {
		entries := grouped[reqID]
		fmt.Printf("\n=== Logs for %s ===\n", reqID)
		for _, e := range entries {
			fmt.Printf("%s: %s\n", e.Timestamp.Format("15:04:05.000"), e.Message)
		}
	}
	fmt.Println("Test completed!")
}

var requestCounter int64 = 100

func generateRequestId() string {
	id := atomic.AddInt64(&requestCounter, 1) - 1 // increment atomically
	return fmt.Sprintf("%06d", id)
	// return fmt.Sprintf("%06d", rand.Intn(1000000)) // 6 digit random ID
}
