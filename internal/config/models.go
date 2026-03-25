package config

import "time"

type Migration struct {
	Processing Processing
}

type Processing struct {
	DeleteNoTTL   DeleteNoTTL
	SplitClusters SplitClusters
}

type DeleteNoTTL struct {
	Enabled   bool
	BatchSize int64
}

type SplitClusters struct {
	Enabled    bool
	BatchSize  int64
	Expiration time.Duration
}
