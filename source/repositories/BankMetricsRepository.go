package repositories

import (
	"context"
	"tps-service/structs"
)

type BankMetricsRepository interface {
	GetBankMetrics(bankId string) (*structs.BankMetrics, error)

	// GetLockBankMetrics(bankId string) (*structs.BankMetrics, error)

	UpdateBankMetrics(bankId string, bankMetrics *structs.BankMetrics) (string, error)

	LockBankMetrics(ctx context.Context, id string) bool

	UnlockBankMetrics(id string) bool

	WaitForUnlock(ctx context.Context, bankId string) bool
}
