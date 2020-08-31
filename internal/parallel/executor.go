package parallel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/event"
	"github.com/meshplus/bitxhub-core/validator"
	"github.com/meshplus/bitxhub-kit/cache"
	"github.com/meshplus/bitxhub-kit/types"
	"github.com/meshplus/bitxhub-model/pb"
	"github.com/meshplus/bitxhub/internal/constant"
	"github.com/meshplus/bitxhub/internal/executor"
	"github.com/meshplus/bitxhub/internal/executor/contracts"
	"github.com/meshplus/bitxhub/internal/ledger"
	"github.com/meshplus/bitxhub/internal/model/events"
	"github.com/meshplus/bitxhub/pkg/vm/boltvm"
	"github.com/sirupsen/logrus"
	"github.com/wasmerio/go-ext-wasm/wasmer"
)

const (
	blockChanNumber   = 1024
	persistChanNumber = 1024
	contractPollSize  = 500
)

var _ executor.Executor = (*ParallelBlockExecutor)(nil)

// ParallelBlockExecutor executes block from order
type ParallelBlockExecutor struct {
	ledger                 ledger.Ledger
	logger                 logrus.FieldLogger
	blockC                 chan *pb.Block
	persistC               chan *ledger.BlockData
	pendingBlockQ          *cache.Cache
	interchainCounter      map[string][]uint64
	counterMux             sync.Mutex
	normalTxs              []types.Hash
	normalTxsMux           sync.Mutex
	validationEngine       validator.Engine
	currentHeight          uint64
	currentBlockHash       types.Hash
	boltContracts          map[string]boltvm.Contract
	interchainContractPool *sync.Pool
	wasmInstances          map[string]wasmer.Instance

	blockFeed event.Feed

	ctx    context.Context
	cancel context.CancelFunc
}

// New creates executor instance
func New(chainLedger ledger.Ledger, logger logrus.FieldLogger) (*ParallelBlockExecutor, error) {
	pendingBlockQ, err := cache.NewCache()
	if err != nil {
		return nil, fmt.Errorf("create cache: %w", err)
	}

	ve := validator.NewValidationEngine(chainLedger, logger)

	boltContracts := registerBoltContracts()
	interchainContractPool := &sync.Pool{
		New: func() interface{} {
			return &contracts.ParallelInterchainManager{}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &ParallelBlockExecutor{
		ledger:                 chainLedger,
		logger:                 logger,
		interchainCounter:      make(map[string][]uint64),
		ctx:                    ctx,
		cancel:                 cancel,
		blockC:                 make(chan *pb.Block, blockChanNumber),
		persistC:               make(chan *ledger.BlockData, persistChanNumber),
		pendingBlockQ:          pendingBlockQ,
		validationEngine:       ve,
		currentHeight:          chainLedger.GetChainMeta().Height,
		currentBlockHash:       chainLedger.GetChainMeta().BlockHash,
		boltContracts:          boltContracts,
		interchainContractPool: interchainContractPool,
		wasmInstances:          make(map[string]wasmer.Instance),
	}, nil
}

// Start starts executor
func (exec *ParallelBlockExecutor) Start() error {
	go exec.listenExecuteEvent()

	go exec.persistData()

	exec.logger.WithFields(logrus.Fields{
		"height": exec.currentHeight,
		"hash":   exec.currentBlockHash.ShortString(),
	}).Infof("ParallelBlockExecutor started")

	return nil
}

// Stop stops executor
func (exec *ParallelBlockExecutor) Stop() error {
	exec.cancel()

	exec.logger.Info("ParallelBlockExecutor stopped")

	return nil
}

// ExecuteBlock executes block from order
func (exec *ParallelBlockExecutor) ExecuteBlock(block *pb.Block) {
	exec.blockC <- block
}

func (exec *ParallelBlockExecutor) SyncExecuteBlock(block *pb.Block) {
	exec.handleExecuteEvent(block)
}

// SubscribeBlockEvent registers a subscription of NewBlockEvent.
func (exec *ParallelBlockExecutor) SubscribeBlockEvent(ch chan<- events.NewBlockEvent) event.Subscription {
	return exec.blockFeed.Subscribe(ch)
}

func (exec *ParallelBlockExecutor) ApplyReadonlyTransactions(txs []*pb.Transaction) []*pb.Receipt {
	current := time.Now()
	receipts := make([]*pb.Receipt, 0, len(txs))

	for i, tx := range txs {
		receipt := &pb.Receipt{
			Version: tx.Version,
			TxHash:  tx.TransactionHash,
		}

		ret, err := exec.applyTransaction(i, tx, nil)
		if err != nil {
			receiptFail(receipt, err)
		} else {
			receiptSuccess(receipt, ret)
		}

		receipts = append(receipts, receipt)
		// clear potential write to ledger
		exec.ledger.Clear()
	}

	exec.logger.WithFields(logrus.Fields{
		"time":  time.Since(current),
		"count": len(txs),
	}).Debug("Apply readonly transactions elapsed")

	return receipts
}

func (exec *ParallelBlockExecutor) listenExecuteEvent() {
	for {
		select {
		case block := <-exec.blockC:
			exec.handleExecuteEvent(block)
		case <-exec.ctx.Done():
			close(exec.persistC)
			return
		}
	}
}

func (exec *ParallelBlockExecutor) persistData() {
	for data := range exec.persistC {
		exec.ledger.PersistBlockData(data)
	}
}

func registerBoltContracts() map[string]boltvm.Contract {
	boltContracts := []*boltvm.BoltContract{
		{
			Enabled:  true,
			Name:     "interchain manager contract",
			Address:  constant.ParallelInterchainContractAddr.String(),
			Contract: &contracts.ParallelInterchainManager{},
		},
		{
			Enabled:  true,
			Name:     "store service",
			Address:  constant.StoreContractAddr.String(),
			Contract: &contracts.Store{},
		},
		{
			Enabled:  true,
			Name:     "rule manager service",
			Address:  constant.RuleManagerContractAddr.String(),
			Contract: &contracts.RuleManager{},
		},
		{
			Enabled:  true,
			Name:     "role manager service",
			Address:  constant.RoleContractAddr.String(),
			Contract: &contracts.Role{},
		},
		{
			Enabled:  true,
			Name:     "appchain manager service",
			Address:  constant.AppchainMgrContractAddr.String(),
			Contract: &contracts.AppchainManager{},
		},
		{
			Enabled:  true,
			Name:     "transaction manager service",
			Address:  constant.TransactionMgrContractAddr.String(),
			Contract: &contracts.TransactionManager{},
		},
		{
			Enabled:  true,
			Name:     "asset exchange service",
			Address:  constant.AssetExchangeContractAddr.String(),
			Contract: &contracts.AssetExchange{},
		},
	}

	return boltvm.Register(boltContracts)
}
