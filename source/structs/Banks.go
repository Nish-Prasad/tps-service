package structs

type Banks struct {
	ID                     int                 `json:"id"`
	BankIfsc               string              `json:"bankIfsc"`
	BankName               string              `json:"bankName"`
	BankID                 string              `json:"bankId"`
	CorpAccNo              string              `json:"corpAccNo"`
	AccountType            string              `json:"accountType"`
	MaxTps                 int64               `json:"maxTps"`
}

func NewBanks(id int, bankIfsc, bankName, bankId string) Banks {
	return Banks{
		ID:                  id,
		BankIfsc:            bankIfsc,
		BankName:            bankName,
		BankID:              bankId,
		MaxTps:              100,
	}
}
