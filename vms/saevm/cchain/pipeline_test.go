// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cchain

import (
	"math/big"
	"testing"

	"github.com/Chaintable/pipeline/processor"
	"github.com/Chaintable/pipeline/tracer"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanchego/utils/logging"

	ptypes "github.com/Chaintable/pipeline/types"
)

// withPusher sets the pipeline's global pusher for the duration of the test.
// Only fields that don't require S3 or Kafka are used.
func withPusher(t *testing.T, last *ptypes.BlockContext, isBackup bool) {
	t.Helper()
	p := &processor.PushProcessor{IsBackup: isBackup}
	if last != nil {
		p.LastBlockNotice = &ptypes.BlockChangeNotification{
			ChangeType: 1,
			NewBlocks:  []ptypes.BlockContext{*last},
		}
	}
	prev := tracer.NodeXPusher
	tracer.NodeXPusher = p
	t.Cleanup(func() { tracer.NodeXPusher = prev })
}

// testChain writes a canonical chain of headers, and a fork of it, to a
// database.
type testChain struct {
	canonical, fork []*types.Header
}

func newTestChain(t *testing.T, db ethdb.KeyValueWriter, canonicalLen, forkFrom, forkLen int) *testChain {
	t.Helper()
	build := func(parent *types.Header, n int, salt byte) []*types.Header {
		var hs []*types.Header
		for range n {
			h := &types.Header{
				Number:     new(big.Int).Add(parent.Number, common.Big1),
				ParentHash: parent.Hash(),
				Time:       parent.Time + 1,
				Extra:      []byte{salt},
				Difficulty: common.Big1,
			}
			rawdb.WriteHeader(db, h)
			hs = append(hs, h)
			parent = h
		}
		return hs
	}
	genesis := &types.Header{Number: common.Big0, Difficulty: common.Big1}
	rawdb.WriteHeader(db, genesis)

	c := &testChain{canonical: append([]*types.Header{genesis}, build(genesis, canonicalLen, 'c')...)}
	c.fork = append(c.canonical[:forkFrom+1:forkFrom+1], build(c.canonical[forkFrom], forkLen, 'f')...)
	return c
}

// uploadedHeaders fakes the headers uploaded to S3.
func uploadedHeaders(hs ...*types.Header) func(common.Hash) (ptypes.BlockContext, error) {
	m := make(map[common.Hash]ptypes.BlockContext)
	for _, h := range hs {
		m[h.Hash()] = blockContext(h)
	}
	return func(h common.Hash) (ptypes.BlockContext, error) {
		c, ok := m[h]
		if !ok {
			return ptypes.BlockContext{}, errHeaderNotFound
		}
		return c, nil
	}
}

func contexts(hs ...*types.Header) []ptypes.BlockContext {
	var cs []ptypes.BlockContext
	for _, h := range hs {
		cs = append(cs, blockContext(h))
	}
	return cs
}

func TestPipelineTracerBlockChange(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	// genesis <- c1 <- c2 <- c3 <- c4
	//               \- f2 <- f3
	c := newTestChain(t, db, 4, 1, 2)
	canon, fork := c.canonical, c.fork
	head := types.NewBlockWithHeader(canon[4])
	higher := types.CopyHeader(canon[4])
	higher.Number = big.NewInt(5)

	tests := []struct {
		name     string
		last     *types.Header
		isBackup bool
		uploaded []*types.Header
		want     *ptypes.BlockChangeNotification
		wantErr  error
	}{
		{
			name: "child of last pushed",
			last: canon[3],
			want: &ptypes.BlockChangeNotification{ChangeType: 1, NewBlocks: contexts(canon[4])},
		},
		{
			name:     "descendant of last pushed",
			last:     canon[1],
			uploaded: canon,
			want:     &ptypes.BlockChangeNotification{ChangeType: 1, NewBlocks: contexts(canon[2], canon[3], canon[4])},
		},
		{
			name:     "descendant of last pushed with untraced blocks",
			last:     canon[1],
			uploaded: canon[:3],
			wantErr:  errUntracedBlock,
		},
		{
			name:     "last pushed on a fork",
			last:     fork[3],
			uploaded: canon,
			want: &ptypes.BlockChangeNotification{
				ChangeType: 2,
				NewBlocks:  contexts(canon[2], canon[3], canon[4]),
				DropBlocks: contexts(fork[2], fork[3]),
			},
		},
		{
			name: "already pushed",
			last: canon[4],
		},
		{
			name: "last pushed is higher",
			last: higher,
		},
		{
			name: "no last pushed",
		},
		{
			name:     "backup",
			last:     canon[3],
			isBackup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var last *ptypes.BlockContext
			if tt.last != nil {
				l := blockContext(tt.last)
				last = &l
			}
			withPusher(t, last, tt.isBackup)

			pt := &pipelineTracer{db: db, log: logging.NoLog{}, uploadedHeader: uploadedHeaders(tt.uploaded...)}
			got, err := pt.blockChange(head)
			require.ErrorIs(t, err, tt.wantErr, "blockChange()")
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPipelineTracerBlockChangeMissingHeader(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	c := newTestChain(t, db, 3, 0, 0)
	unknown := blockContext(&types.Header{Number: big.NewInt(1), Extra: []byte("unknown")})
	withPusher(t, &unknown, false)

	pt := &pipelineTracer{db: db, log: logging.NoLog{}}
	// The unknown block's ancestry is looked up in S3, for which the pusher
	// has no client, so the notification MUST NOT be built.
	got, err := pt.blockChange(types.NewBlockWithHeader(c.canonical[3]))
	require.ErrorIs(t, err, errHeaderNotFound)
	assert.Nil(t, got)
}

func TestPipelineTracerShouldTrace(t *testing.T) {
	block := types.NewBlockWithHeader(&types.Header{Number: big.NewInt(10), Difficulty: common.Big1})
	other := types.NewBlockWithHeader(&types.Header{Number: big.NewInt(10), Difficulty: common.Big2})

	tests := []struct {
		name string
		last *ptypes.BlockContext
		want bool
	}{
		{name: "nothing pushed", want: true},
		{name: "pushed below", last: &ptypes.BlockContext{BlockNumber: 9}, want: true},
		{name: "pushed", last: &ptypes.BlockContext{BlockNumber: 10, Hash: block.Hash()}, want: false},
		{name: "sibling pushed", last: &ptypes.BlockContext{BlockNumber: 10, Hash: other.Hash()}, want: true},
		{name: "pushed above", last: &ptypes.BlockContext{BlockNumber: 11}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withPusher(t, tt.last, false)
			pt := &pipelineTracer{}
			assert.Equal(t, tt.want, pt.ShouldTrace(block))
		})
	}
}

func TestPipelineTracerExportsExecutedHeader(t *testing.T) {
	hdr := &types.Header{
		Number:     big.NewInt(42),
		Difficulty: common.Big1,
		Root:       common.Hash{'s', 'e', 't', 't', 'l', 'e', 'd'},
		BaseFee:    big.NewInt(1000), // worst case
	}
	block := types.NewBlockWithHeader(hdr)
	t.Cleanup(func() { tracer.BlockCtx = nil })

	inner, err := tracer.NewPipelineTracer(tracer.PipelineTracerConfig{})
	require.NoError(t, err)
	pt := &pipelineTracer{PipelineTracer: inner}

	executedBaseFee := big.NewInt(7)
	pt.OnBlockStart(block, executedBaseFee)
	executedBaseFee.SetInt64(8) // MUST have been copied

	ctx := tracer.BlockCtx
	assert.Equal(t, block.Hash(), ctx.BlockHeader.Hash, "header hash")
	assert.Equal(t, hdr.Root, ctx.BlockHeader.StateRoot, "state root before execution")
	assert.Equal(t, int64(7), ctx.BlockHeader.BaseFeePerGas.ToInt().Int64(), "header base fee")
	assert.Equal(t, int64(7), ctx.BlockFile.Block.BaseFeePerGas.Int64(), "block file base fee")
	assert.Equal(t, int64(1000), hdr.BaseFee.Int64(), "original header MUST NOT be modified")
}
