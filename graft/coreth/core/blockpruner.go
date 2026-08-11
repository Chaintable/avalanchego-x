// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package core

import (
	"encoding/binary"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/log"
	"github.com/ava-labs/libevm/metrics"
	"github.com/ava-labs/libevm/rlp"

	"github.com/ava-labs/avalanchego/vms/evm/sync/customrawdb"
)

var (
	blockPruneTimer     = metrics.GetOrRegisterTimer("chain/block/prune", nil)
	prunedBlocksCounter = metrics.GetOrRegisterCounter("chain/block/pruned", nil)
	prunedTailGauge     = metrics.GetOrRegisterGauge("chain/block/prune/tail", nil)

	// bodyPrefix and receiptsPrefix mirror the (unexported) libevm rawdb schema:
	// bodies are stored under "b" + num (8 byte big endian) + hash and receipts
	// under "r" + num (8 byte big endian) + hash. The layout is frozen by
	// BlockChainVersion and iterating it by height is the only way to prune a
	// height range without a freezer.
	bodyPrefix     = []byte("b")
	receiptsPrefix = []byte("r")
)

const (
	// prunerBlocksPerBatch is the number of heights processed per write batch.
	// Deletes count as size 1 in a batch regardless of value size, so flushing
	// is driven by block count (mirroring rawdb.UnindexTransactions) rather
	// than batch.ValueSize.
	prunerBlocksPerBatch = 1000

	// prunerCompactionChunk is the number of pruned heights after which the
	// deleted key ranges are compacted to reclaim disk space during the
	// initial catch-up. In steady state (one height per accepted block) this
	// threshold is never reached and compaction is left to the database.
	prunerCompactionChunk = 1_000_000

	// prunerFinalCompactionMin is the minimum number of heights pruned in a
	// single run for a trailing compaction to be worthwhile.
	prunerFinalCompactionMin = 100_000

	prunerProgressLog = 8 * time.Second
)

// blockPruner deletes bodies, receipts, total difficulty and transaction
// lookup indices of blocks below the configured retention window. Headers and
// canonical hash mappings are always retained, as is the genesis block.
//
// The pruned tail state lives on the BlockChain (loaded from its persisted
// marker even when pruning is disabled) so RPC guards keep rejecting queries
// for pruned heights after the pruner is turned off.
type blockPruner struct {
	// window is the number of most recent blocks whose history is retained:
	// [HEAD-window+1, HEAD]. Must be non-zero (a zero window disables pruning
	// and the pruner is never constructed).
	window uint64
	db     ethdb.Database
	term   chan chan struct{}
	closed chan struct{}

	chain *BlockChain
}

// newBlockPruner initializes the block history pruner and starts its
// background loop.
func newBlockPruner(window uint64, chain *BlockChain) *blockPruner {
	pruner := &blockPruner{
		window: window,
		db:     chain.db,
		term:   make(chan chan struct{}),
		closed: make(chan struct{}),
		chain:  chain,
	}
	tail := max(chain.historyPrunedTail.Load(), 1)
	prunedTailGauge.Update(int64(tail))

	chain.wg.Add(1)
	go func() {
		defer chain.wg.Done()
		pruner.loop(chain)
	}()

	log.Info("Initialized block history pruner", "window", window, "tail", tail)
	return pruner
}

// loop is the scheduler of the pruner, launching pruning tasks as the accepted
// chain advances. It mirrors txIndexer.loop: a single background task runs at
// a time and the accepted event feed is never blocked.
func (p *blockPruner) loop(chain *BlockChain) {
	defer close(p.closed)

	var (
		stop        chan struct{} // Non-nil if background routine is active.
		done        chan struct{} // Non-nil if background routine is active.
		lastHead    uint64        // The latest announced accepted head.
		runningHead uint64        // The head number being processed in background.

		headCh = make(chan ChainEvent)
		sub    = chain.SubscribeChainAcceptedEvent(headCh)
	)
	if sub == nil {
		log.Warn("could not create chain accepted subscription to prune block history")
		return
	}
	defer sub.Unsubscribe()

	// startRun launches the background pruning task.
	startRun := func(newHead uint64) {
		stop = make(chan struct{})
		done = make(chan struct{})
		runningHead = newHead
		p.chain.wg.Add(1)
		go func() {
			defer p.chain.wg.Done()
			p.run(runningHead, stop, done)
		}()
	}

	// Launch the initial processing if the chain already extends beyond the
	// retention window. This is the catch-up entry point when pruning is first
	// enabled on a node with existing history.
	if head := p.chain.CurrentBlock(); head != nil && head.Number.Uint64() > p.window {
		startRun(head.Number.Uint64())
	}
	for {
		select {
		case head := <-headCh:
			headNum := head.Block.NumberU64()
			if headNum < p.window {
				break
			}

			// If no background task is running, start a new one.
			// We cannot block on the subscription channel because it can
			// cause a fatal error.
			if done == nil {
				startRun(headNum)
			}
			lastHead = headNum
		case <-done:
			stop = nil
			done = nil
			// If a new head arrived during the last run, start a new one.
			if runningHead < lastHead {
				startRun(lastHead)
			}
		case ch := <-p.term:
			if stop != nil {
				close(stop)
			}
			if done != nil {
				log.Info("Waiting for background block history pruner to exit")
				<-done
			}
			close(ch)
			return
		}
	}
}

// close shuts down the pruner. Safe to be called multiple times.
func (p *blockPruner) close() {
	ch := make(chan struct{})
	select {
	case p.term <- ch:
		<-ch
	case <-p.closed:
	}
}

// run prunes all block history below [head - window + 1] in batches. Progress
// is persisted with every batch via the pruned block tail marker, so an
// interrupted run (shutdown or crash) resumes where it left off.
func (p *blockPruner) run(head uint64, stop chan struct{}, done chan struct{}) {
	start := time.Now()
	defer func() {
		blockPruneTimer.Update(time.Since(start))
		close(done)
	}()

	if head < p.window {
		return
	}
	// target is the first height to retain.
	target := head - p.window + 1
	tail := max(p.chain.historyPrunedTail.Load(), 1)
	if tail >= target {
		return
	}

	var (
		runStart     = tail
		compactStart = tail
		logged       = time.Now()
	)
	for tail < target {
		select {
		case <-stop:
			return
		case <-p.chain.quit:
			return
		default:
		}

		// Skip over ranges with no data (state synced nodes never stored most
		// of their history) instead of iterating them height by height: jump
		// straight to the next height at which a body or receipts record
		// exists and advance the marker in a single write.
		next, err := p.nextPrunableHeight(tail, target)
		if err != nil {
			log.Error("Failed to scan for prunable block history", "tail", tail, "target", target, "err", err)
			return
		}
		batchEnd := min(tail+prunerBlocksPerBatch, target)
		if next > tail {
			batchEnd = next
		}
		if err := p.pruneRange(tail, batchEnd); err != nil {
			log.Error("Failed to prune block history", "tail", tail, "target", target, "err", err)
			return
		}
		prunedBlocksCounter.Inc(int64(batchEnd - tail))
		prunedTailGauge.Update(int64(batchEnd))
		tail = batchEnd

		if time.Since(logged) > prunerProgressLog {
			logged = time.Now()
			log.Info("Pruning block history", "tail", tail, "target", target, "elapsed", common.PrettyDuration(time.Since(start)))
		}
		// Compact the fully deleted range chunk by chunk during catch-up to
		// reclaim disk space eagerly.
		if tail-compactStart >= prunerCompactionChunk {
			p.compactRange(compactStart, tail)
			compactStart = tail
		}
	}
	// Trailing compaction after a large catch-up run.
	if tail-compactStart >= prunerFinalCompactionMin {
		p.compactRange(compactStart, tail)
	}
	if tail > runStart {
		log.Info("Pruned block history", "from", runStart, "to", tail, "elapsed", common.PrettyDuration(time.Since(start)))
	}
}

// nextPrunableHeight returns the lowest height in [from, target) at which a
// body or receipts record exists, or target if the whole range is empty.
func (p *blockPruner) nextPrunableHeight(from, target uint64) (uint64, error) {
	next := target
	for _, prefix := range [][]byte{bodyPrefix, receiptsPrefix} {
		it := p.db.NewIterator(prefix, encodeBlockNumber(from))
		if it.Next() {
			if num, _, ok := parseNumberHashKey(prefix, it.Key()); ok && num < next {
				next = num
			}
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return 0, err
		}
	}
	return next, nil
}

// pruneRange deletes the block history of all blocks (canonical or not) in
// heights [from, to) within a single write batch and atomically advances the
// pruned block tail marker to [to]. The tx index tail is advanced along with
// it so the tx indexer never attempts to read a pruned body.
func (p *blockPruner) pruneRange(from, to uint64) error {
	p.chain.txIndexTailLock.Lock()
	defer p.chain.txIndexTailLock.Unlock()

	batch := p.db.NewBatch()

	// Delete tx lookup entries, bodies and total difficulties. Iterating the
	// body prefix by height covers canonical blocks as well as stale siblings
	// left behind by a crash.
	it := p.db.NewIterator(bodyPrefix, encodeBlockNumber(from))
	for it.Next() {
		num, hash, ok := parseNumberHashKey(bodyPrefix, it.Key())
		if !ok {
			continue
		}
		if num >= to {
			break
		}
		body := new(types.Body)
		if err := rlp.DecodeBytes(it.Value(), body); err != nil {
			log.Warn("Failed to decode block body during history pruning", "number", num, "hash", hash, "err", err)
		} else {
			canonical := rawdb.ReadCanonicalHash(p.db, num) == hash
			for _, tx := range body.Transactions {
				// A transaction in a stale sibling may have been accepted in a
				// later canonical block that is still retained; its lookup
				// entry points there and must survive. (A transaction of a
				// canonical block cannot recur in another canonical block, so
				// its lookup is always safe to delete.)
				if !canonical {
					if lookup := rawdb.ReadTxLookupEntry(p.db, tx.Hash()); lookup != nil && *lookup >= to {
						continue
					}
				}
				rawdb.DeleteTxLookupEntry(batch, tx.Hash())
			}
		}
		if err := batch.Delete(it.Key()); err != nil {
			it.Release()
			return err
		}
		rawdb.DeleteTd(batch, hash, num)
	}
	if err := it.Error(); err != nil {
		it.Release()
		return err
	}
	it.Release()

	// Delete all receipts in range, including orphans whose body is already
	// missing.
	it = p.db.NewIterator(receiptsPrefix, encodeBlockNumber(from))
	for it.Next() {
		num, _, ok := parseNumberHashKey(receiptsPrefix, it.Key())
		if !ok {
			continue
		}
		if num >= to {
			break
		}
		if err := batch.Delete(it.Key()); err != nil {
			it.Release()
			return err
		}
	}
	if err := it.Error(); err != nil {
		it.Release()
		return err
	}
	it.Release()

	if err := customrawdb.WritePrunedBlockTail(batch, to); err != nil {
		return err
	}
	// Maintain the invariant TxIndexTail >= PrunedBlockTail. Reading the tail
	// from the database (not the batch) is safe: all writers hold
	// txIndexTailLock.
	if curr := rawdb.ReadTxIndexTail(p.db); curr == nil || *curr < to {
		rawdb.WriteTxIndexTail(batch, to)
	}
	if err := batch.Write(); err != nil {
		return err
	}
	p.chain.historyPrunedTail.Store(to)
	return nil
}

// compactRange compacts the body and receipt key ranges for heights
// [from, to) to reclaim the space freed by pruning.
func (p *blockPruner) compactRange(from, to uint64) {
	start := time.Now()
	for _, prefix := range [][]byte{bodyPrefix, receiptsPrefix} {
		if err := p.db.Compact(
			append(prefix, encodeBlockNumber(from)...),
			append(prefix, encodeBlockNumber(to)...),
		); err != nil {
			log.Warn("Failed to compact pruned block history", "from", from, "to", to, "err", err)
			return
		}
	}
	log.Info("Compacted pruned block history", "from", from, "to", to, "elapsed", common.PrettyDuration(time.Since(start)))
}

// encodeBlockNumber encodes a block number as 8 bytes big endian, matching the
// rawdb schema.
func encodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

// parseNumberHashKey parses a rawdb key of the form
// prefix + num (8 byte big endian) + hash, returning ok = false for keys of a
// different shape (e.g. the canonical hash suffix keys sharing the "h"
// prefix).
func parseNumberHashKey(prefix, key []byte) (uint64, common.Hash, bool) {
	if len(key) != len(prefix)+8+common.HashLength {
		return 0, common.Hash{}, false
	}
	number := binary.BigEndian.Uint64(key[len(prefix) : len(prefix)+8])
	hash := common.BytesToHash(key[len(prefix)+8:])
	return number, hash, true
}
