// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package customrawdb

import (
	"testing"

	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/stretchr/testify/require"
)

func TestPrunedBlockTail(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	// Absent marker reads as zero without error.
	tail, err := ReadPrunedBlockTail(db)
	require.NoError(t, err)
	require.Zero(t, tail)

	// Round-trip.
	require.NoError(t, WritePrunedBlockTail(db, 90_001))
	tail, err = ReadPrunedBlockTail(db)
	require.NoError(t, err)
	require.Equal(t, uint64(90_001), tail)

	// Overwrite advances the marker.
	require.NoError(t, WritePrunedBlockTail(db, 123_456))
	tail, err = ReadPrunedBlockTail(db)
	require.NoError(t, err)
	require.Equal(t, uint64(123_456), tail)

	// Delete restores the unpruned state.
	require.NoError(t, DeletePrunedBlockTail(db))
	tail, err = ReadPrunedBlockTail(db)
	require.NoError(t, err)
	require.Zero(t, tail)

	// Malformed values surface errInvalidData.
	require.NoError(t, db.Put(prunedBlockTailKey, []byte("bad")))
	_, err = ReadPrunedBlockTail(db)
	require.ErrorIs(t, err, errInvalidData)
}
