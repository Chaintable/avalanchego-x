// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package config

const (
	TxGossipBloomMinTargetElements       = 8 * 1024
	TxGossipBloomTargetFalsePositiveRate = 0.01
	TxGossipBloomResetFalsePositiveRate  = 0.05
	TxGossipBloomChurnMultiplier         = 3

	// MinBlockHistory is the smallest non-zero retention window accepted for
	// block-history. It must comfortably cover the deepest internal reader of
	// historical bodies: crash recovery replays up to 2*commit-interval (8192)
	// blocks and the state sync server serves blocks back to the previous
	// syncable boundary (up to 2*state-sync-commit-interval = 32768).
	MinBlockHistory uint64 = 32_768
)
