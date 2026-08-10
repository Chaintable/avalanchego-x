// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package filters

import (
	"context"
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/stretchr/testify/require"
)

// TestFilterPrunedHistory ensures log queries below the pruned history tail
// fail explicitly instead of silently returning incomplete results.
func TestFilterPrunedHistory(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	backend, sys := newTestFilterSystem(t, db, Config{})
	backend.historyPrunedTail = 100

	// Range queries starting below the tail are rejected.
	_, err := sys.NewRangeFilter(99, 120, nil, nil).Logs(context.Background())
	require.ErrorContains(t, err, "historical logs have been pruned")
	require.ErrorContains(t, err, "earliest queryable is 100")

	// Block queries for a pruned height are rejected.
	prunedHeader := &types.Header{Number: big.NewInt(42)}
	rawdb.WriteHeader(db, prunedHeader)
	_, err = sys.NewBlockFilter(prunedHeader.Hash(), nil, nil).Logs(context.Background())
	require.ErrorContains(t, err, "historical logs have been pruned")

	// Queries at or above the tail pass the guard (no results, but no
	// pruning error either).
	logs, err := sys.NewRangeFilter(100, 100, nil, nil).Logs(context.Background())
	require.NoError(t, err)
	require.Empty(t, logs)
}
