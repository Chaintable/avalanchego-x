// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package core

import (
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethdb"
	ethparams "github.com/ava-labs/libevm/params"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanchego/graft/coreth/consensus/dummy"
	"github.com/ava-labs/avalanchego/graft/coreth/params"
	"github.com/ava-labs/avalanchego/vms/evm/sync/customrawdb"
)

func blockPrunerTestCacheConfig() *CacheConfig {
	return &CacheConfig{
		TrieCleanLimit:            256,
		TrieDirtyLimit:            256,
		TrieDirtyCommitTarget:     20,
		TriePrefetcherParallelism: 4,
		Pruning:                   true,
		CommitInterval:            4096,
		StateHistory:              32,
		SnapshotLimit:             256,
		SnapshotNoBuild:           true, // Ensure the test errors if snapshot initialization fails
		AcceptorQueueLimit:        64,
	}
}

// generatePrunerTestChain generates a chain of [n] blocks, each holding one
// transaction, on a fresh genesis.
func generatePrunerTestChain(t *testing.T, n int) (*Genesis, types.Blocks, types.Blocks) {
	require := require.New(t)
	key1, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	key2, _ := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	addr1 := crypto.PubkeyToAddress(key1.PublicKey)
	addr2 := crypto.PubkeyToAddress(key2.PublicKey)
	gspec := &Genesis{
		Config: &params.ChainConfig{HomesteadBlock: new(big.Int)},
		Alloc:  types.GenesisAlloc{addr1: {Balance: big.NewInt(10000000000000)}},
	}
	signer := types.LatestSigner(gspec.Config)
	addTx := func(i int, block *BlockGen) {
		tx, err := types.SignTx(types.NewTransaction(block.TxNonce(addr1), addr2, big.NewInt(10000), ethparams.TxGas, nil, nil), signer, key1)
		require.NoError(err)
		block.AddTx(tx)
	}
	genDb, blocks, _, err := GenerateChainWithGenesis(gspec, dummy.NewFakerWithCallbacks(TestCallbacks), n, 10, addTx)
	require.NoError(err)
	extraBlocks, _, err := GenerateChain(gspec.Config, blocks[len(blocks)-1], dummy.NewFakerWithCallbacks(TestCallbacks), genDb, 5, 10, addTx)
	require.NoError(err)
	return gspec, blocks, extraBlocks
}

func waitForPrunedTail(t *testing.T, db ethdb.Database, expected uint64) {
	require.Eventually(t, func() bool {
		tail, err := customrawdb.ReadPrunedBlockTail(db)
		require.NoError(t, err)
		return tail == expected
	}, 30*time.Second, 10*time.Millisecond, "pruned block tail did not reach %d", expected)
}

// checkBlockPruned asserts that the body, receipts, td and tx lookup entries
// of [block] are deleted while its header and canonical mapping are retained.
func checkBlockPruned(t *testing.T, db ethdb.Database, block *types.Block) {
	require := require.New(t)
	num, hash := block.NumberU64(), block.Hash()
	require.Zero(len(rawdb.ReadBodyRLP(db, hash, num)), "body %d should be pruned", num)
	require.Zero(len(rawdb.ReadReceiptsRLP(db, hash, num)), "receipts %d should be pruned", num)
	require.Nil(rawdb.ReadTd(db, hash, num), "td %d should be pruned", num)
	for _, tx := range block.Transactions() {
		require.Nil(rawdb.ReadTxLookupEntry(db, tx.Hash()), "tx lookup %s should be pruned", tx.Hash())
	}
	require.NotNil(rawdb.ReadHeader(db, hash, num), "header %d must be retained", num)
	require.Equal(hash, rawdb.ReadCanonicalHash(db, num), "canonical mapping %d must be retained", num)
}

// checkBlockRetained asserts the full block history of [block] is present.
func checkBlockRetained(t *testing.T, db ethdb.Database, block *types.Block) {
	require := require.New(t)
	num, hash := block.NumberU64(), block.Hash()
	require.NotZero(len(rawdb.ReadBodyRLP(db, hash, num)), "body %d must be retained", num)
	require.NotZero(len(rawdb.ReadReceiptsRLP(db, hash, num)), "receipts %d must be retained", num)
	for _, tx := range block.Transactions() {
		require.NotNil(rawdb.ReadTxLookupEntry(db, tx.Hash()), "tx lookup %s must be retained", tx.Hash())
	}
}

func TestBlockHistoryPruning(t *testing.T) {
	require := require.New(t)
	gspec, blocks, extraBlocks := generatePrunerTestChain(t, 128)

	conf := blockPrunerTestCacheConfig()
	conf.BlockHistory = 32

	chainDB := rawdb.NewMemoryDatabase()
	// Plant an orphan receipt (no matching body) below the retention window to
	// exercise the fallback receipts sweep.
	orphanHash := common.HexToHash("0xdeadbeef")
	rawdb.WriteReceipts(chainDB, orphanHash, 5, types.Receipts{})

	chain, err := createAndInsertChain(chainDB, conf, gspec, blocks, common.Hash{}, nil)
	require.NoError(err)
	defer chain.Stop()

	head := blocks[len(blocks)-1].NumberU64()
	expectedTail := head - conf.BlockHistory + 1
	waitForPrunedTail(t, chainDB, expectedTail)
	require.Equal(expectedTail, chain.HistoryPrunedTail())

	// Everything below the tail is pruned, headers retained.
	for _, block := range blocks {
		if block.NumberU64() < expectedTail {
			checkBlockPruned(t, chainDB, block)
		} else {
			checkBlockRetained(t, chainDB, block)
		}
	}
	// The genesis block is never pruned.
	require.NotZero(len(rawdb.ReadBodyRLP(chainDB, chain.genesisBlock.Hash(), 0)))

	// The orphan receipt is swept.
	require.Zero(len(rawdb.ReadReceiptsRLP(chainDB, orphanHash, 5)))

	// The tx index tail is advanced along with the pruned tail.
	txTail := rawdb.ReadTxIndexTail(chainDB)
	require.NotNil(txTail)
	require.GreaterOrEqual(*txTail, expectedTail)

	// Accepting another block advances the tail by one.
	_, err = chain.InsertChain(extraBlocks[:1])
	require.NoError(err)
	require.NoError(chain.Accept(extraBlocks[0]))
	chain.DrainAcceptorQueue()
	waitForPrunedTail(t, chainDB, expectedTail+1)
}

func TestBlockHistoryPruningCatchUp(t *testing.T) {
	require := require.New(t)
	gspec, blocks, _ := generatePrunerTestChain(t, 128)

	// Build the chain with pruning disabled.
	conf := blockPrunerTestCacheConfig()
	chainDB := rawdb.NewMemoryDatabase()
	chain, err := createAndInsertChain(chainDB, conf, gspec, blocks, common.Hash{}, nil)
	require.NoError(err)
	lastAccepted := blocks[len(blocks)-1]
	chain.Stop()
	tail, err := customrawdb.ReadPrunedBlockTail(chainDB)
	require.NoError(err)
	require.Zero(tail, "no marker while pruning is disabled")

	// Reopen with pruning enabled: startup catch-up prunes the backlog.
	conf.BlockHistory = 64
	chain, err = createBlockChain(chainDB, conf, gspec, lastAccepted.Hash())
	require.NoError(err)
	expectedTail := lastAccepted.NumberU64() - conf.BlockHistory + 1
	waitForPrunedTail(t, chainDB, expectedTail)
	chain.Stop()

	// Reopen with a smaller window: catch-up resumes from the stored marker.
	conf.BlockHistory = 32
	chain, err = createBlockChain(chainDB, conf, gspec, lastAccepted.Hash())
	require.NoError(err)
	expectedTail = lastAccepted.NumberU64() - conf.BlockHistory + 1
	waitForPrunedTail(t, chainDB, expectedTail)
	chain.Stop()

	for _, block := range blocks {
		if block.NumberU64() < expectedTail {
			checkBlockPruned(t, chainDB, block)
		} else {
			checkBlockRetained(t, chainDB, block)
		}
	}
}

func TestBlockHistoryPruningSiblingLookup(t *testing.T) {
	require := require.New(t)
	gspec, blocks, _ := generatePrunerTestChain(t, 128)

	// Build the chain with pruning disabled so all lookups exist.
	conf := blockPrunerTestCacheConfig()
	chainDB := rawdb.NewMemoryDatabase()
	chain, err := createAndInsertChain(chainDB, conf, gspec, blocks, common.Hash{}, nil)
	require.NoError(err)
	lastAccepted := blocks[len(blocks)-1]
	chain.Stop()

	// Plant a stale sibling body at a to-be-pruned height containing a tx
	// that was canonically accepted at a retained height. Its lookup entry
	// points at the retained height and must survive pruning of the sibling.
	retainedTx := blocks[110].Transactions()[0] // height 111, retained with window 32
	siblingHash := common.HexToHash("0xabcd")
	rawdb.WriteBody(chainDB, siblingHash, 5, &types.Body{Transactions: types.Transactions{retainedTx}})

	conf.BlockHistory = 32
	chain, err = createBlockChain(chainDB, conf, gspec, lastAccepted.Hash())
	require.NoError(err)
	defer chain.Stop()

	expectedTail := lastAccepted.NumberU64() - conf.BlockHistory + 1
	waitForPrunedTail(t, chainDB, expectedTail)

	// The sibling body is deleted but the retained tx lookup survives.
	require.Zero(len(rawdb.ReadBodyRLP(chainDB, siblingHash, 5)))
	lookup := rawdb.ReadTxLookupEntry(chainDB, retainedTx.Hash())
	require.NotNil(lookup)
	require.Equal(uint64(111), *lookup)
}

func TestHistoryPrunedTailPersistsWhenDisabled(t *testing.T) {
	require := require.New(t)
	gspec, blocks, _ := generatePrunerTestChain(t, 128)

	conf := blockPrunerTestCacheConfig()
	conf.BlockHistory = 32
	chainDB := rawdb.NewMemoryDatabase()
	chain, err := createAndInsertChain(chainDB, conf, gspec, blocks, common.Hash{}, nil)
	require.NoError(err)
	lastAccepted := blocks[len(blocks)-1]
	expectedTail := lastAccepted.NumberU64() - conf.BlockHistory + 1
	waitForPrunedTail(t, chainDB, expectedTail)
	chain.Stop()

	// Reopen with pruning disabled: previously pruned heights must remain
	// reported so RPC guards keep rejecting them.
	conf.BlockHistory = 0
	chain, err = createBlockChain(chainDB, conf, gspec, lastAccepted.Hash())
	require.NoError(err)
	defer chain.Stop()
	require.Equal(expectedTail, chain.HistoryPrunedTail())
}

func TestBlockHistoryPruningWithTxIndexer(t *testing.T) {
	require := require.New(t)
	gspec, blocks, _ := generatePrunerTestChain(t, 128)

	conf := blockPrunerTestCacheConfig()
	conf.BlockHistory = 64
	conf.TransactionHistory = 16

	chainDB := rawdb.NewMemoryDatabase()
	chain, err := createAndInsertChain(chainDB, conf, gspec, blocks, common.Hash{}, nil)
	require.NoError(err)
	defer chain.Stop()

	head := blocks[len(blocks)-1].NumberU64()
	prunedTail := head - conf.BlockHistory + 1
	waitForPrunedTail(t, chainDB, prunedTail)

	// The tx indexer's unindexing boundary must never fall below the pruned
	// tail (it would read deleted bodies otherwise).
	require.Eventually(func() bool {
		txTail := rawdb.ReadTxIndexTail(chainDB)
		return txTail != nil && *txTail == head-conf.TransactionHistory+1
	}, 30*time.Second, 10*time.Millisecond)
	txTail := rawdb.ReadTxIndexTail(chainDB)
	require.GreaterOrEqual(*txTail, prunedTail)
}

func TestBlockHistoryPruningDisabled(t *testing.T) {
	require := require.New(t)
	gspec, blocks, _ := generatePrunerTestChain(t, 32)

	conf := blockPrunerTestCacheConfig()
	chainDB := rawdb.NewMemoryDatabase()
	chain, err := createAndInsertChain(chainDB, conf, gspec, blocks, common.Hash{}, nil)
	require.NoError(err)
	defer chain.Stop()

	require.Zero(chain.HistoryPrunedTail())
	tail, err := customrawdb.ReadPrunedBlockTail(chainDB)
	require.NoError(err)
	require.Zero(tail)
	for _, block := range blocks {
		checkBlockRetained(t, chainDB, block)
	}
}
