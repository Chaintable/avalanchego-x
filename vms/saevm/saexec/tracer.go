// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package saexec

import (
	"math/big"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
)

// A Tracer observes canonical block execution, e.g. to export executed blocks
// to an external data pipeline. It is only ever invoked by the [Executor], never
// for historical or speculative execution, and MUST NOT modify execution.
//
// All methods are called from the single execution goroutine. For every block
// that [Tracer.ShouldTrace] accepts, the order of calls is:
//
//  1. [Tracer.OnBlockStart];
//  2. for each transaction: [Tracer.OnTxStart], the [vm.EVMLogger] and
//     [Tracer.OnLog] callbacks, then [Tracer.OnTxEnd];
//  3. [Tracer.OnCommit], only if the post-execution state root differs from
//     the parent's; and
//  4. [Tracer.OnBlockEnd], after the post-execution state is committed but
//     before the block is marked as executed.
type Tracer interface {
	vm.EVMLogger

	// ShouldTrace reports whether the block's execution should be traced.
	// Blocks that are re-executed during recovery and were already exported
	// MAY be skipped.
	ShouldTrace(*types.Block) bool
	// OnBlockStart is called before any transaction is executed. The base fee
	// is the one actually used for execution, which, unlike synchronous
	// blocks, is not necessarily the one in the block header.
	OnBlockStart(b *types.Block, baseFee *big.Int)
	OnTxStart(tx *types.Transaction, from common.Address)
	// OnTxEnd is called with the receipt after it has been populated with the
	// block hash and effective gas price.
	OnTxEnd(receipt *types.Receipt, err error)
	OnLog(*types.Log)
	// OnCommit has the signature of [state.StateDB.OnCommit] and is called
	// when the post-execution state is committed.
	OnCommit(
		originRoot, root common.Hash,
		destructs map[common.Hash]struct{},
		accounts map[common.Hash][]byte,
		accountsOrigin map[common.Address][]byte,
		storages map[common.Hash]map[common.Hash][]byte,
		storagesOrigin map[common.Address]map[common.Hash][]byte,
		codes map[common.Hash][]byte,
	)
	// OnBlockEnd is called with the parent's and the block's post-execution
	// state roots once the latter is committed.
	OnBlockEnd(b *types.Block, parentRoot, root common.Hash)
}

// An ExecutorOption configures an [Executor] constructed by [New].
type ExecutorOption func(*Executor)

// WithTracer configures the [Executor] to trace canonical block execution. A
// nil [Tracer] disables tracing.
func WithTracer(t Tracer) ExecutorOption {
	return func(e *Executor) {
		e.tracer = t
	}
}
