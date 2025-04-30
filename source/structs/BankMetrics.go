package structs

import (
	"encoding/json"
	"sync"
	"time"
)

const (
	successRateTimeRange = time.Hour
	maxLogSize           = 1000
	refillInterval       = time.Second
)

var (
	GlobalBankMetricMap = make(map[string]*BankMetrics)
)

var (
	BankTickers = make(map[string]*time.Ticker)
	TickerMu    sync.Mutex
)

type BankMetrics struct {
	BankId             string
	TotalTransaction   int64
	SuccessTransaction int64
	SuccessRate        float64
	CurrentTps         int64
	MaxTps             int64
	LastUpdated        time.Time
	IsAvailable        bool
	DowntimeCount      int64
	TransactionLog     []Transaction
	Tokens             int64
	LastRefillTime     time.Time
	TokenAvailable     chan struct{}
	StopRefillChan     chan struct{}
	TickerRunning      bool
	Mu                 sync.Mutex
	once               sync.Once
}

type Transaction struct {
	Timestamp time.Time
	Success   bool
}

func NewBankMetrics(bankId string) *BankMetrics {
	return &BankMetrics{
		BankId:         bankId,
		MaxTps:         10,
		IsAvailable:    true,
		LastUpdated:    time.Now(),
		TransactionLog: make([]Transaction, 0),
		Tokens:         10,
		TokenAvailable: make(chan struct{}, 1),
		StopRefillChan: make(chan struct{}),
	}
}

func (bm *BankMetrics) UpdateSuccess() bool {

	bm.TotalTransaction++
	bm.SuccessTransaction++
	bm.addTransaction(time.Now(), true)
	bm.CurrentTps++

	bm.recalculateSuccessRate()
	return true
}

func (bm *BankMetrics) UpdateFailed() bool {

	bm.TotalTransaction++
	bm.addTransaction(time.Now(), false)

	bm.recalculateSuccessRate()
	return true
}

func (bm *BankMetrics) recalculateSuccessRate() {
	bm.Mu.Lock()
	defer bm.Mu.Unlock()
	
	cutoff := time.Now().Add(-successRateTimeRange)
	validTransactions := 0
	successfulTransactions := 0

	newTransactionLog := make([]Transaction, 0, len(bm.TransactionLog))
	for _, txn := range bm.TransactionLog {
		if txn.Timestamp.After(cutoff) {
			newTransactionLog = append(newTransactionLog, txn)
			validTransactions++
			if txn.Success {
				successfulTransactions++
			}
		}
	}

	bm.TransactionLog = newTransactionLog

	// Avoid division by zero
	if validTransactions > 0 {
		bm.SuccessRate = (float64(successfulTransactions) / float64(validTransactions)) * 100
	} else {
		bm.SuccessRate = 0.0
	}
}


func (bm *BankMetrics) addTransaction(timestamp time.Time, success bool) {
	bm.TransactionLog = append(bm.TransactionLog, Transaction{Timestamp: timestamp, Success: success})

	if len(bm.TransactionLog) > maxLogSize {
		bm.TransactionLog = bm.TransactionLog[len(bm.TransactionLog)-maxLogSize:]
	}
}

func (bm *BankMetrics) UpdateAvailability(isAvailable bool) {

	if !isAvailable {
		bm.DowntimeCount++
	} else {
		if bm.DowntimeCount > 0 {
			bm.DowntimeCount--
		}
	}

	bm.IsAvailable = isAvailable
}

func (bm *BankMetrics) MarshalJSON() ([]byte, error) {
	type Alias struct {
		BankId             string
		TotalTransaction   int64
		SuccessTransaction int64
		SuccessRate        float64
		CurrentTps         int64
		MaxTps             int64
		LastUpdated        time.Time
		IsAvailable        bool
		DowntimeCount      int64
		TransactionLog     []Transaction
		Tokens             int64
		LastRefillTime     time.Time
		TickerRunning      bool
	}

	return json.Marshal(&Alias{
		BankId:             bm.BankId,
		TotalTransaction:   bm.TotalTransaction,
		SuccessTransaction: bm.SuccessTransaction,
		SuccessRate:        bm.SuccessRate,
		CurrentTps:         bm.CurrentTps,
		MaxTps:             bm.MaxTps,
		LastUpdated:        bm.LastUpdated,
		IsAvailable:        bm.IsAvailable,
		DowntimeCount:      bm.DowntimeCount,
		TransactionLog:     bm.TransactionLog,
		Tokens:             bm.Tokens,
		LastRefillTime:     bm.LastRefillTime,
		TickerRunning:      bm.TickerRunning,
	})
}
