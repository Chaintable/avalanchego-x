// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cchain

import (
	"math/big"
	"testing"

	"github.com/Chaintable/pipeline/processor"
	"github.com/Chaintable/pipeline/tracer"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ptypes "github.com/Chaintable/pipeline/types"
)

// withPusher sets the pipeline's global pusher for the duration of the test.
// Only fields that don't require S3 or Kafka are used.
func withPusher(t *testing.T, last *ptypes.BlockContext) {
	t.Helper()
	p := &processor.PushProcessor{}
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
			withPusher(t, tt.last)
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

	isBackup := true
	inner, err := tracer.NewPipelineTracer(tracer.PipelineTracerConfig{IsBackup: &isBackup})
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
