package tracing

import (
	"github.com/Chaintable/pipeline/tracer"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/params"
)

type (
	/*
		- VM events -
	*/

	// TxStartHook is called before the execution of a transaction starts.
	// Call simulations don't come with a valid signature. `from` field
	// to be used for address of the caller.
	TxStartHook = func(tx *types.Transaction, from common.Address)

	// TxEndHook is called after the execution of a transaction ends.
	TxEndHook = func(receipt *types.Receipt, err error)

	// BlockchainInitHook is called when the blockchain is initialized.
	BlockchainInitHook = func(chainConfig *params.ChainConfig)

	CloseHook = func()

	// BlockStartHook is called before executing `block`.
	// `td` is the total difficulty prior to `block`.
	BlockStartHook = func(block *types.Block)

	// BlockEndHook is called after executing a block.
	BlockEndHook = func(err error)

	// GenesisBlockHook is called when the genesis block is being processed.
	GenesisBlockHook = func(genesis *types.Block, alloc types.GenesisAlloc)

	// CommitHook is called when the state is committed.
	CommitHook = func(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, accountsOrigin map[common.Address][]byte, storages map[common.Hash]map[common.Hash][]byte, storagesOrigin map[common.Address]map[common.Hash][]byte, codes map[common.Hash][]byte)

	// LogHook is called when a log is emitted.
	LogHook = func(log *types.Log)
)

type Hooks struct {
	// VM events
	OnTxStart TxStartHook
	OnTxEnd   TxEndHook
	// Chain events
	OnBlockchainInit BlockchainInitHook
	OnClose          CloseHook
	OnBlockStart     BlockStartHook
	OnBlockEnd       BlockEndHook
	OnGenesisBlock   GenesisBlockHook
	OnLog            LogHook
	// custom hook
	OnCommit CommitHook
}

func BuildHooks(t *tracer.PipelineTracer) *Hooks {
	return &Hooks{
		OnBlockchainInit: t.OnBlockchainInit,
		OnClose:          t.OnClose,
		OnBlockStart:     t.OnBlockStart,
		OnTxStart:        t.OnTxStart,
		OnTxEnd:          t.OnTxEnd,
		OnLog:            t.OnLog,
		OnGenesisBlock:   t.OnGenesisBlock,
		OnCommit:         t.OnCommit,
	}
}
