package service

import (
	// "context"
	"container/list"
	"context"
	"fmt"
	"time"
	"tps-service/repositories"
	"tps-service/structs"
)

type TpsService struct {
	redisRepo      repositories.BankMetricsRepository
	TokenAvailable chan string
	StopRefillChan chan struct{}
}

func GetTpsService() *TpsService {
	if tpsService == nil {
		tpsService = &TpsService{
			redisRepo:      NewRedisBankMetricsRepository(redisClient),
			TokenAvailable: make(chan string, 1),
			StopRefillChan: make(chan struct{}),
		}
	}
	return tpsService
}

var tpsService *TpsService

func StartRefillTicker(bankId string, tps *TpsService) {
	structs.TickerMu.Lock()
	defer structs.TickerMu.Unlock()

	if _, exists := structs.BankTickers[bankId]; exists {
		return
	}

	fmt.Printf("Starting refill ticker for %s\n", bankId)
	ticker := time.NewTicker(time.Minute)
	ctx := context.Background()

	structs.BankTickers[bankId] = ticker
	go func(bankId string, tps *TpsService) {
		defer func() {
			ticker.Stop()
			structs.TickerMu.Lock()
			delete(structs.BankTickers, bankId)
			structs.TickerMu.Unlock()
			tps.redisRepo.UnlockBankMetrics(bankId)
			// fmt.Printf("Stopped ticker and released lock for %s\n", bankId)
		}()

		for {
			select {
			case <-ticker.C:
				lockCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				if tps.redisRepo.LockBankMetrics(lockCtx, bankId) {
					bm, err := tps.redisRepo.GetBankMetrics(bankId)
					if err != nil {
						fmt.Printf("[%s] Not Found\n", bankId)
					}
					fmt.Printf("[%s] Refilling Token\n", bankId)
					bm.Mu.Lock()
					if bm.Tokens < bm.MaxTps {
						bm.Tokens = bm.MaxTps
						bm.LastRefillTime = time.Now()
						tps.redisRepo.UpdateBankMetrics(bankId, bm)
						fmt.Printf("Refilled tokens for %s\n", bankId)
						select {
						case tps.TokenAvailable <- bankId:
							fmt.Printf("Signal Sent for %s\n", bankId)
						default:
							fmt.Printf("Signal Dropped for %s\n", bankId)
						}
					}
					bm.Mu.Unlock()
					tps.redisRepo.UnlockBankMetrics(bankId)
				} else {
					// fmt.Printf("Skipping refill, lock not acquired: %s\n", bankId)
				}
				cancel()
			case <-tps.StopRefillChan:
				// fmt.Printf("Stopping ticker for %s\n", bankId)
				return
			}
		}
	}(bankId, tps)
}

func TryConsumeToken(bankMap map[string]*structs.Banks, bankList *list.List, requestId string, tps *TpsService) *structs.BankMetrics {
	var priorityBankId string
	// Convert slice to linked list
	for {
		bm, consumed := tryConsumeTokenOnce(bankMap, bankList, requestId, priorityBankId)
		if bm != nil && consumed {
			return bm
		}
		fmt.Print("\n -------------------------Signal Wait----------------------------\n")
		bankId := <-tps.TokenAvailable
		fmt.Printf("\n -------------------------HEY[%s]--------------------------", bankId)
		priorityBankId = bankId
		// Optional: small sleep to avoid tight loop
		time.Sleep(10 * time.Millisecond)
	}
}

// --- tries ONCE ---
func tryConsumeTokenOnce(bankMap map[string]*structs.Banks, bankList *list.List, requestId string, priorityBankId string) (*structs.BankMetrics, bool) {
	tps := GetTpsService()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		bank *structs.Banks
		ok   bool
	}

	resultChan := make(chan result, 1)

	if priorityBankId != "" {
		fmt.Printf("\n -------------------------PRIORITY[%s]--------------------------", priorityBankId)
		if bank, ok := bankMap[priorityBankId]; ok && tps.redisRepo.LockBankMetrics(ctx, priorityBankId) {
			// try consuming token from bank
			bm, err := tps.redisRepo.GetBankMetrics(bank.BankID)
			if err != nil {
				fmt.Printf("[%s] Not Found\n", priorityBankId)
			}
			fmt.Printf("Trying to consume from priority: %s | Tokens: %d\n", priorityBankId, bm.Tokens)
			bm.Mu.Lock()
			if bm.Tokens < 0 {
				bm.Mu.Unlock()
				tps.redisRepo.UnlockBankMetrics(bm.BankId)
			} else {
				Log(requestId, fmt.Sprintf("[Bank %s] Lock acquired immediately\n", priorityBankId))
				fmt.Printf("[Bank %s] Lock acquired immediately\n", priorityBankId)
				if consumeTokenAndReturn(bm, tps, requestId) {
					cancel()
					return bm, true
				}
			}
		}
	}

	for e := bankList.Front(); e != nil; {
		bank := e.Value.(*structs.Banks)
		bankId := bank.BankID
		next := e.Next() // Store next before possibly moving node

		if tps.redisRepo.LockBankMetrics(ctx, bankId) {
			// cancel() // Cancel others
			bm, err := tps.redisRepo.GetBankMetrics(bank.BankID)
			if err != nil {
				fmt.Printf("[%s] Not Found\n", bankId)
				e = next
				continue
			}
			bm.Mu.Lock()
			token := bm.Tokens
			if token <= 0 {
				bm.Mu.Unlock()
				tps.redisRepo.UnlockBankMetrics(bm.BankId)
				bankList.MoveToBack(e)
				e = next
				continue
			} else {
				Log(requestId, fmt.Sprintf("[Bank %s] Lock acquired immediately\n", bankId))
				fmt.Printf("[Bank %s] Lock acquired immediately\n", bankId)
				if consumeTokenAndReturn(bm, tps, requestId) {
					cancel()
					return bm, true
				}
			}
		} else {
			// fmt.Printf("[Bank %s] Lock not acquired immediately, starting wait\n", bankId)
			go func(bank *structs.Banks) {
				var locked bool
				select {
				case <-ctx.Done():
					return
				default:
					// fmt.Printf("\n[%s] WaitForUnlock started\n", bank.BankID)
					// locked = tps.redisRepo.WaitForUnlock(ctx, bank.BankID)
					// fmt.Printf("\n[%s] WaitForUnlock returned: %v\n", bank.BankID, locked)
				}

				if ctx.Err() != nil {
					// if locked {
					// 	// fmt.Printf("[Bank %s] Unlocking due to cancellation\n", bm.BankId)
					// 	tps.redisRepo.UnlockBankMetrics(bank.BankID)
					// }
					return
				}

				fmt.Printf("\n[%s] WaitForUnlock started\n", bank.BankID)
				locked = tps.redisRepo.WaitForUnlock(ctx, bank.BankID)
				fmt.Printf("\n[%s] WaitForUnlock returned: %v\n", bank.BankID, locked)

				if locked {
					fmt.Printf("[%s] Trying to send result to channel\n", bank.BankID)
					select {
					case resultChan <- result{bank, true}:
						fmt.Printf("[%s] Sent result to channel\n", bank.BankID)
					default:
						fmt.Printf("[%s] Skipped sending result to channel\n", bank.BankID)
					}
				}
			}(bank)
		}
		e = next
	}

	select {
	case res := <-resultChan:
		bankId := res.bank.BankID
		Log(requestId, fmt.Sprintf("[Bank %s] Got lock after wait\n", bankId))
		fmt.Printf("[%s][Bank %s] Got lock after wait\n", requestId, bankId)
		cancel() // stop others
		bm, err := tps.redisRepo.GetBankMetrics(bankId)
		if err != nil {
			fmt.Printf("[%s] Not Found\n", bankId)
		}
		bm.Mu.Lock()
		if bm.Tokens <= 0 {
			bm.Mu.Unlock()
			tps.redisRepo.UnlockBankMetrics(bankId)
			return bm, false
		} else {
			if consumeTokenAndReturn(bm, tps, requestId) {
				cancel()
				return bm, true
			}
		}
	}
	return nil, false
}

// Helper to consume token once lock acquired
func consumeTokenAndReturn(bm *structs.BankMetrics, tps *TpsService, requestId string) bool {
	// defer tps.redisRepo.UnlockBankMetrics(ctx, bm.BankId)
	bankId := bm.BankId
	// bm.Mu.Lock()
	Log(requestId, fmt.Sprintf("\nTotal Token:[%s] %d\n", bankId, bm.Tokens))
	// fmt.Printf("\n[%s]Total Token:[%s] %d\n",requestId, bankId, bm.Tokens)
	if bm.Tokens > 0 {
		bm.Tokens--
		tps.redisRepo.UpdateBankMetrics(bankId, bm)
		bm.Mu.Unlock()
		tps.redisRepo.UnlockBankMetrics(bankId)
		Log(requestId, fmt.Sprintf("\nToken Left:[%s] %d\n", bankId, bm.Tokens))
		// for id, bm := range structs.GlobalBankMetricMap {
		// 	if id != bankId {
		// 		Log(requestId, fmt.Sprintf("Other Bank ID: %s, Tokens: %d\n", id, bm.Tokens))
		// 		// fmt.Printf("Other Bank ID: %s, Tokens: %d\n", id, bm.Tokens)
		// 	}
		// }
		return true
	}
	bm.Mu.Unlock()
	tps.redisRepo.UnlockBankMetrics(bankId)
	return false
	// for {
	// 	select {
	// 	case <-bm.TokenAvailable:
	// 		return false
	// 	}
	// }
}

// func TryConsumeToken(bm *structs.BankMetrics, last bool) bool {
// 	tps := GetTpsService()
// 	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	// defer cancel()
// 	ctx := context.Background()

// 	maxRetries := 10
// 	retryCount := 0
// 	bankId := bm.BankId

// 	for retryCount < maxRetries {
// 		if !tps.redisRepo.WaitForUnlock(ctx, bankId) {
// 			retryCount++
// 			continue
// 		}

// 		bm.Mu.Lock()
// 		if bm.Tokens > 0 {
// 			bm.Tokens--
// 			tps.redisRepo.UpdateBankMetrics(bm.BankId, bm)
// 			bm.Mu.Unlock()
// 			tps.redisRepo.UnlockBankMetrics(ctx, bankId)
// 			fmt.Printf("\nToken Left: %d\n", bm.Tokens)
// 			return true
// 		}
// 		bm.Mu.Unlock()
// 		tps.redisRepo.UnlockBankMetrics(ctx, bankId)

// 		fmt.Println("Waiting for token:", bankId)
// 		select {
// 		case <-bm.TokenAvailable:
// 			continue // Try again when a token becomes available
// 		}
// 	}
// 	fmt.Println("Max retry limit reached, exiting...")
// 	return false
// }

func StopRefillTicker(bankId string) {
	structs.TickerMu.Lock()
	defer structs.TickerMu.Unlock()

	if ticker, exists := structs.BankTickers[bankId]; exists {
		ticker.Stop()
		delete(structs.BankTickers, bankId)
		fmt.Printf("Stopped ticker for %s\n", bankId)
	}
}

// func calculateBankPriority(bankId string, bankMap map[string]*structs.Banks) int {
//     bank := bankMap[bankId]
//     if bank == nil {
//         return 0 // Unknown bank, very low priority
//     }
//     bm, err := GetTpsService().redisRepo.GetBankMetrics(bank.BankID)
//     if err != nil {
//         return 0
//     }

//     score := bm.SuccessRate*10 + bm.Tokens
//     return score
// }

// func refillTokens(bm *structs.BankMetrics, tps *TpsService, ctx context.Context) {
// 	if !tps.redisRepo.LockBankMetrics(ctx, bm.BankId) {
// 		fmt.Printf("Skipping refill, lock not acquired: %s\n", bm.BankId)
// 		return
// 	}
// 	bm.Mu.Lock()
// 	defer bm.Mu.Unlock()
// 	defer tps.redisRepo.UnlockBankMetrics(ctx, bm.BankId)

// 	if bm.Tokens == 0 {
// 		refillAmount := bm.MaxTps
// 		bm.Tokens += refillAmount
// 		if bm.Tokens > bm.MaxTps {
// 			bm.Tokens = bm.MaxTps
// 		}
// 		bm.LastRefillTime = time.Now()
// 		tps.redisRepo.UpdateBankMetrics(bm.BankId, bm)
// 		select {
// 		case bm.TokenAvailable <- struct{}{}:
// 		default:
// 		}
// 		fmt.Printf("Refilled tokens for %s: %d\n", bm.BankId, bm.Tokens)
// 	}
// }
