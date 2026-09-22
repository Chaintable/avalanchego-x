// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package saexec

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanchego/vms/saevm/saetest"
)

// A tracerEvent records a single [Tracer] call. Only the fields relevant to
// the call's kind are populated.
type tracerEvent struct {
	Kind string

	Block   common.Hash
	BaseFee *big.Int

	Tx   common.Hash
	From common.Address

	ReceiptBlock common.Hash
	GasPrice     *big.Int

	LogAddress common.Address

	Origin, Root common.Hash
	LastExecuted common.Hash
}

// recordingTracer records all [Tracer] calls other than per-opcode ones.
type recordingTracer struct {
	exec   *Executor
	skip   map[uint64]bool
	events []tracerEvent
}

var _ Tracer = (*recordingTracer)(nil)

func (r *recordingTracer) add(e tracerEvent) { r.events = append(r.events, e) }

func (r *recordingTracer) ShouldTrace(b *types.Block) bool { return !r.skip[b.NumberU64()] }

func (r *recordingTracer) OnBlockStart(b *types.Block, baseFee *big.Int) {
	r.add(tracerEvent{Kind: "block_start", Block: b.Hash(), BaseFee: baseFee})
}

func (r *recordingTracer) OnTxStart(tx *types.Transaction, from common.Address) {
	r.add(tracerEvent{Kind: "tx_start", Tx: tx.Hash(), From: from})
}

func (r *recordingTracer) OnTxEnd(receipt *types.Receipt, err error) {
	r.add(tracerEvent{
		Kind:         "tx_end",
		Tx:           receipt.TxHash,
		ReceiptBlock: receipt.BlockHash,
		GasPrice:     receipt.EffectiveGasPrice,
	})
}

func (r *recordingTracer) OnLog(l *types.Log) {
	r.add(tracerEvent{Kind: "log", LogAddress: l.Address})
}

func (r *recordingTracer) OnCommit(originRoot, root common.Hash, _ map[common.Hash]struct{}, _ map[common.Hash][]byte, _ map[common.Address][]byte, _ map[common.Hash]map[common.Hash][]byte, _ map[common.Address]map[common.Hash][]byte, _ map[common.Hash][]byte) {
	r.add(tracerEvent{Kind: "commit", Origin: originRoot, Root: root})
}

func (r *recordingTracer) OnBlockEnd(b *types.Block, parentRoot, root common.Hash) {
	r.add(tracerEvent{
		Kind:         "block_end",
		Block:        b.Hash(),
		Origin:       parentRoot,
		Root:         root,
		LastExecuted: r.exec.LastExecuted().Hash(),
	})
}

func (r *recordingTracer) CaptureStart(*vm.EVM, common.Address, common.Address, bool, []byte, uint64, *big.Int) {
	r.add(tracerEvent{Kind: "capture_start"})
}
func (*recordingTracer) CaptureTxStart(uint64)            {}
func (*recordingTracer) CaptureTxEnd(uint64)              {}
func (*recordingTracer) CaptureEnd([]byte, uint64, error) {}
func (*recordingTracer) CaptureEnter(vm.OpCode, common.Address, common.Address, []byte, uint64, *big.Int) {
}
func (*recordingTracer) CaptureExit([]byte, uint64, error) {}
func (*recordingTracer) CaptureState(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, []byte, int, error) {
}
func (*recordingTracer) CaptureFault(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, int, error) {
}

func TestTracer(t *testing.T) {
	logger := common.Address{'l', 'o', 'g'}
	rec := &recordingTracer{skip: make(map[uint64]bool)}
	ctx, sut := newSUT(t,
		withExecutorOptions(WithTracer(rec)),
		withExtraAlloc(types.GenesisAlloc{
			logger: {
				Code:    saetest.LogTopOfStackAfter(saetest.Ops(vm.PUSH0)),
				Balance: big.NewInt(0),
			},
		}),
	)
	rec.exec = sut.Executor
	e, chain, wallet := sut.Executor, sut.chain, sut.wallet
	from := wallet.Addresses()[0]

	callLogger := wallet.SetNonceAndSign(t, 0, &types.LegacyTx{
		To:       &logger,
		Gas:      1e5,
		GasPrice: big.NewInt(1),
	})
	transfer := wallet.SetNonceAndSign(t, 0, &types.LegacyTx{
		To:       &common.Address{'e', 'o', 'a'},
		Value:    big.NewInt(1),
		Gas:      1e5,
		GasPrice: big.NewInt(1),
	})
	withTxs := chain.NewBlock(t, types.Transactions{callLogger, transfer})
	empty := chain.NewBlock(t, nil)
	skipped := chain.NewBlock(t, types.Transactions{
		wallet.SetNonceAndSign(t, 0, &types.LegacyTx{
			To:       &common.Address{'e', 'o', 'a'},
			Value:    big.NewInt(1),
			Gas:      1e5,
			GasPrice: big.NewInt(1),
		}),
	})
	rec.skip[skipped.NumberU64()] = true

	genesisRoot := withTxs.ParentBlock().PostExecutionStateRoot()
	for _, b := range chain.AllExceptGenesis() {
		require.NoError(t, e.Enqueue(ctx, b), "Enqueue()")
	}
	require.NoErrorf(t, skipped.WaitUntilExecuted(ctx), "%T.WaitUntilExecuted()", skipped)

	withTxsRoot := withTxs.PostExecutionStateRoot()
	require.NotEqual(t, genesisRoot, withTxsRoot, "state root of block with transactions")
	require.Equal(t, withTxsRoot, empty.PostExecutionStateRoot(), "state root of empty block")
	receipts := withTxs.Receipts()
	require.Len(t, receipts, 2)

	want := []tracerEvent{
		{Kind: "block_start", Block: withTxs.Hash(), BaseFee: withTxs.ExecutedBaseFee().ToBig()},
		{Kind: "tx_start", Tx: callLogger.Hash(), From: from},
		{Kind: "capture_start"},
		{Kind: "log", LogAddress: logger},
		{Kind: "tx_end", Tx: callLogger.Hash(), ReceiptBlock: withTxs.Hash(), GasPrice: receipts[0].EffectiveGasPrice},
		{Kind: "tx_start", Tx: transfer.Hash(), From: from},
		{Kind: "capture_start"},
		{Kind: "tx_end", Tx: transfer.Hash(), ReceiptBlock: withTxs.Hash(), GasPrice: receipts[1].EffectiveGasPrice},
		{Kind: "commit", Origin: genesisRoot, Root: withTxsRoot},
		// The block MUST NOT be marked as executed before the tracer is done.
		{Kind: "block_end", Block: withTxs.Hash(), Origin: genesisRoot, Root: withTxsRoot, LastExecuted: withTxs.ParentHash()},

		{Kind: "block_start", Block: empty.Hash(), BaseFee: empty.ExecutedBaseFee().ToBig()},
		// No commit because the state root is unchanged.
		{Kind: "block_end", Block: empty.Hash(), Origin: withTxsRoot, Root: withTxsRoot, LastExecuted: withTxs.Hash()},
		// Nothing from the skipped block.
	}
	for _, r := range receipts {
		require.NotNilf(t, r.EffectiveGasPrice, "%T.EffectiveGasPrice", r)
	}
	bigEq := cmp.Comparer(func(a, b *big.Int) bool {
		if a == nil || b == nil {
			return a == b
		}
		return a.Cmp(b) == 0
	})
	if diff := cmp.Diff(want, rec.events, bigEq); diff != "" {
		t.Errorf("tracer events diff (-want +got):\n%s", diff)
	}
}
