// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package customrawdb

import (
	"encoding/binary"
	"fmt"

	"github.com/ava-labs/libevm/ethdb"
)

// prunedBlockTailKey tracks the first block number whose body, receipts, total
// difficulty and transaction lookup indices are still present on disk. All
// blocks in the range [1, tail) have had these records deleted by the online
// block history pruner. Headers and canonical hash mappings are always
// retained, and the genesis block (height 0) is never pruned. A missing marker
// means no history has ever been pruned.
var prunedBlockTailKey = []byte("PrunedBlockTail")

// WritePrunedBlockTail stores the first block number whose block history is
// retained.
func WritePrunedBlockTail(db ethdb.KeyValueWriter, tail uint64) error {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, tail)
	return db.Put(prunedBlockTailKey, data)
}

// ReadPrunedBlockTail returns the first block number whose block history is
// retained. If the marker is absent (no history has been pruned), it returns
// (0, nil).
func ReadPrunedBlockTail(db ethdb.KeyValueReader) (uint64, error) {
	ok, err := db.Has(prunedBlockTailKey)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	data, err := db.Get(prunedBlockTailKey)
	if err != nil {
		return 0, err
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("%w: length %d", errInvalidData, len(data))
	}
	return binary.BigEndian.Uint64(data), nil
}

// DeletePrunedBlockTail removes the pruned block tail marker. This is intended
// for tests and manual recovery only.
func DeletePrunedBlockTail(db ethdb.KeyValueWriter) error {
	return db.Delete(prunedBlockTailKey)
}
