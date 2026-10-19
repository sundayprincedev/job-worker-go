package types

import "time"

type PaymentPayload struct {
	MerchantID  string
	Amount      int64
	CustomerRef string
}

const (
	HouseAccountID    = "HOUSE_COMM_PH_001"
	CommissionRateBPS = 2000 // 20.00% in basis points
)

var UniversalJobTimeout = 100 * time.Millisecond
