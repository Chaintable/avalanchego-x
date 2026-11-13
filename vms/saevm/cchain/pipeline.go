// Copyright (C) 2019, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cchain

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/Chaintable/pipeline/processor"
	"github.com/Chaintable/pipeline/tracer"
	"github.com/Chaintable/pipeline/util"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/params"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/vms/saevm/saexec"

	ptypes "github.com/Chaintable/pipeline/types"
	corethlog "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/log"
	evmprometheus "github.com/ava-labs/avalanchego/vms/evm/metrics/prometheus"
	ethmetrics "github.com/ava-labs/libevm/metrics"
)

// pipelineConfig holds the options of the Chaintable pipeline tracer, which
// exports executed blocks to S3 and Kafka. The key is shared with coreth so
// that the same config applies before and after the SAE transition.
type pipelineConfig struct {
	VMTraceConfig *tracer.PipelineTracerConfig `json:"vm-trace-config"`
}

var _ saexec.Tracer = (*pipelineTracer)(nil)

const libevmMetricsPrefix = "eth"

// pipelineTracer adapts the pipeline's live tracer, which coreth drives from
// its core.BlockChain, to the SAE [saexec.Executor].
//
// SAE block headers carry the state root of the last-settled block and the
// worst-case base fee, whereas the pipeline's consumers look up a block's state
// diff by its header's state root and execute calls with its header's base
// fee. The exported header therefore carries the post-execution state root and
// the base fee actually used for execution, like the header that SAE's own
// debug tracers see. The block hash is unchanged.
type pipelineTracer struct {
	*tracer.PipelineTracer

	db  ethdb.Reader
	log logging.Logger
	// uploadedHeader reads a header uploaded to S3. If nil,
	// [s3UploadedHeader] is used.
	uploadedHeader func(common.Hash) (ptypes.BlockContext, error)
}

// newPipelineTracer creates the tracer and initializes the pipeline's S3 and
// Kafka clients, which also loads the last block change notification pushed to
// Kafka. The pipeline exits the process if initialization fails.
func newPipelineTracer(
	snowCtx *snow.Context,
	cfg tracer.PipelineTracerConfig,
	chainConfig *params.ChainConfig,
	db ethdb.Reader,
) (*pipelineTracer, error) {
	// The pipeline logs through libevm's root logger, which only coreth
	// configures and which otherwise discards everything.
	alias, err := snowCtx.BCLookup.PrimaryAlias(snowCtx.ChainID)
	if err != nil {
		alias = snowCtx.ChainID.String()
	}
	if _, err := corethlog.InitLogger(alias, "info", false, snowCtx.Log); err != nil {
		return nil, fmt.Errorf("initializing libevm logger: %w", err)
	}

	// The pipeline's metrics, e.g. pipeline/block_time, are recorded in
	// libevm's global registry, which coreth exports under the same prefix.
	if err := snowCtx.Metrics.Register(libevmMetricsPrefix, evmprometheus.NewGatherer(ethmetrics.DefaultRegistry)); err != nil {
		return nil, fmt.Errorf("registering libevm metrics: %w", err)
	}

	t, err := tracer.NewPipelineTracer(cfg)
	if err != nil {
		return nil, err
	}
	t.OnBlockchainInit(chainConfig)

	pt := &pipelineTracer{
		PipelineTracer: t,
		db:             db,
		log:            snowCtx.Log,
	}
	var last any = "none"
	if b := pt.lastPushed(); b != nil {
		last = *b
	}
	snowCtx.Log.Info("pipeline tracer enabled",
		zap.Boolp("backup", cfg.IsBackup),
		zap.String("topic", cfg.Topic),
		zap.Any("lastPushed", last),
	)
	return pt, nil
}

func (*pipelineTracer) lastPushed() *ptypes.BlockContext {
	if tracer.NodeXPusher == nil {
		return nil
	}
	return tracer.NodeXPusher.LastPushedBlock()
}

// ShouldTrace skips blocks that were already pushed to Kafka, which are only
// re-executed during recovery after a restart. Their files were uploaded before
// the notification was pushed.
func (t *pipelineTracer) ShouldTrace(b *types.Block) bool {
	last := t.lastPushed()
	if last == nil {
		return true
	}
	switch n := b.NumberU64(); {
	case n < last.BlockNumber:
		return false
	case n == last.BlockNumber:
		return b.Hash() != last.Hash
	default:
		return true
	}
}

// OnBlockStart overrides [tracer.PipelineTracer.OnBlockStart] to export the
// base fee used for execution.
func (t *pipelineTracer) OnBlockStart(b *types.Block, baseFee *big.Int) {
	t.PipelineTracer.OnBlockStart(b)
	tracer.BlockCtx.BlockHeader.BaseFeePerGas = (*hexutil.Big)(new(big.Int).Set(baseFee))
	tracer.BlockCtx.BlockFile.Block.BaseFeePerGas = new(big.Int).Set(baseFee)
}

// OnCommit overrides [tracer.PipelineTracer.OnCommit] to export the
// post-execution state root, under which the state diff is uploaded.
func (t *pipelineTracer) OnCommit(
	originRoot, root common.Hash,
	destructs map[common.Hash]struct{},
	accounts map[common.Hash][]byte,
	accountsOrigin map[common.Address][]byte,
	storages map[common.Hash]map[common.Hash][]byte,
	storagesOrigin map[common.Address]map[common.Hash][]byte,
	codes map[common.Hash][]byte,
) {
	tracer.BlockCtx.BlockHeader.StateRoot = root
	t.PipelineTracer.OnCommit(originRoot, root, destructs, accounts, accountsOrigin, storages, storagesOrigin, codes)
}

// OnBlockEnd uploads the block's files if the state root didn't change (so
// [pipelineTracer.OnCommit] wasn't called), then notifies the pipeline of the
// block, which pushes the block change notification to Kafka if this node is
// the leader.
func (t *pipelineTracer) OnBlockEnd(b *types.Block, parentRoot, root common.Hash) {
	ctx := tracer.BlockCtx
	ctx.BlockHeader.StateRoot = root
	if !ctx.Committed && parentRoot != root {
		t.log.Error("pipeline tracer missed a state commit",
			zap.Uint64("height", b.NumberU64()),
			zap.Stringer("parentRoot", parentRoot),
			zap.Stringer("root", root),
		)
	}
	t.PipelineTracer.OnBlockEnd(nil)

	// The pipeline owns the leader check, the common ancestor computation and
	// the Kafka write gate; backup nodes return immediately. Accepted SAE
	// blocks are final but coreth pushed preferred blocks before acceptance, so
	// the last pushed block MAY be on a different branch.
	r := &pipelineHeaderReader{t: t}
	if last := t.lastPushed(); last != nil {
		r.lastPushed = last.Hash
	}
	if err := tracer.NodeXPusher.NotifyBlockCommit(b, r, nil); err != nil {
		// The notification is retried with the next block because the last
		// pushed block is unchanged.
		t.log.Error("pushing pipeline block change notification",
			zap.Uint64("height", b.NumberU64()),
			zap.Stringer("hash", b.Hash()),
			zap.Error(err),
		)
	}
}

func blockContext(h *types.Header) ptypes.BlockContext {
	return ptypes.BlockContext{
		BlockNumber: h.Number.Uint64(),
		Hash:        h.Hash(),
		ParentHash:  h.ParentHash,
		Timestamp:   h.Time,
	}
}

// pipelineHeaderReader provides the pipeline with the ancestry of the blocks
// it notifies consumers of.
type pipelineHeaderReader struct {
	t *pipelineTracer
	// lastPushed is the hash of the last pushed block, if any, read before
	// notifying the pipeline. It is exempt from the export check because the
	// pipeline already notified consumers of it.
	lastPushed common.Hash
}

var _ processor.BlockContextReader = (*pipelineHeaderReader)(nil)

// GetHeaderByHash2 implements [processor.BlockChainReader] with the headers in
// the database. The pipeline uses [pipelineHeaderReader.GetBlockContextByHash]
// instead.
func (r *pipelineHeaderReader) GetHeaderByHash2(hash common.Hash) *types.Header {
	if num := rawdb.ReadHeaderNumber(r.t.db, hash); num != nil {
		return rawdb.ReadHeader(r.t.db, hash, *num)
	}
	return nil
}

// GetBlockContextByHash reads the header from the database, falling back to
// the header uploaded to S3, as does coreth's BlockChain.GetHeaderByHash2. The
// S3 header is not decoded as a [types.Header] because its hash is not
// necessarily reproducible from its fields; see [pipelineTracer].
//
// The pipeline only reads the ancestry of a block down to the common ancestor
// with the last pushed block, all of which consumers were or are about to be
// notified of, so it MUST have been exported. Blocks executed without tracing
// weren't.
func (r *pipelineHeaderReader) GetBlockContextByHash(hash common.Hash) (ptypes.BlockContext, error) {
	h := r.GetHeaderByHash2(hash)
	if h == nil {
		return r.t.uploaded(hash)
	}
	c := blockContext(h)
	if hash == r.lastPushed {
		return c, nil
	}
	if _, err := r.t.uploaded(hash); err != nil {
		return ptypes.BlockContext{}, fmt.Errorf("%w: block %d (%v) was never exported; the node MUST be resynced from the state of block %d: %v",
			errUntracedBlock, c.BlockNumber, c.Hash, c.BlockNumber-1, err)
	}
	return c, nil
}

var (
	errHeaderNotFound = errors.New("header not found")
	errUntracedBlock  = errors.New("untraced block")
)

func (t *pipelineTracer) uploaded(hash common.Hash) (ptypes.BlockContext, error) {
	if t.uploadedHeader != nil {
		return t.uploadedHeader(hash)
	}
	return s3UploadedHeader(hash)
}

// s3UploadedHeader reads the header uploaded to S3.
func s3UploadedHeader(hash common.Hash) (ptypes.BlockContext, error) {
	pusher := tracer.NodeXPusher
	if pusher == nil || pusher.Uploader == nil {
		return ptypes.BlockContext{}, fmt.Errorf("%w: %v", errHeaderNotFound, hash)
	}
	var h ptypes.Header
	key := fmt.Sprintf("%s/%s/block", tracer.BizChainID, hash.String())
	if err := util.DownloadFileFromS3Json(pusher.Uploader, pusher.Bucket, key, &h); err != nil {
		return ptypes.BlockContext{}, fmt.Errorf("%w: %v: %v", errHeaderNotFound, hash, err)
	}
	if h.Number == nil || h.Hash != hash {
		return ptypes.BlockContext{}, fmt.Errorf("%w: %v: invalid S3 header %q", errHeaderNotFound, hash, key)
	}
	return ptypes.BlockContext{
		BlockNumber: h.Number.ToInt().Uint64(),
		Hash:        h.Hash,
		ParentHash:  h.ParentHash,
		Timestamp:   uint64(h.Timestamp),
	}, nil
}
