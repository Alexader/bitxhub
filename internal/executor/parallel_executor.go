package executor

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/meshplus/bitxhub-core/agency"
	"github.com/meshplus/bitxhub-kit/types"
	"github.com/meshplus/bitxhub-model/pb"
	"github.com/sirupsen/logrus"
)

var runOnDAG = false

type ParallelExecutor struct {
	normalTxs         []*types.Hash
	interchainCounter map[string][]*pb.VerifiedIndex
	receipts          []*pb.Receipt
	counterMux        sync.Mutex
	normalTxsMux      sync.Mutex
	applyTxFunc       agency.ApplyTxFunc
	boltContractPool  *sync.Pool
	logger            logrus.FieldLogger
}

func NewParallelExecutor(f1 agency.ApplyTxFunc, f2 agency.RegisterContractFunc, logger logrus.FieldLogger) agency.TxsExecutor {
	boltContractPool := &sync.Pool{
		New: func() interface{} {
			return f2()
		},
	}
	return &ParallelExecutor{
		applyTxFunc:      f1,
		boltContractPool: boltContractPool,
		logger:           logger,
	}
}

func init() {
	agency.RegisterExecutorConstructor("parallel", NewParallelExecutor)
}

func (pe *ParallelExecutor) ApplyTransactions(txs []*pb.Transaction) []*pb.Receipt {
	pe.interchainCounter = make(map[string][]*pb.VerifiedIndex)
	pe.normalTxs = make([]*types.Hash, 0)
	pe.receipts = make([]*pb.Receipt, len(txs), len(txs))

	groups, err := groupTxs(txs)
	if err != nil {
		panic(fmt.Sprintf("Group tx: %s", err.Error()))
	}
	//pe.logger.Infof("parallel executor splits txs to %d groups", len(groups))

	for _, group := range groups {
		pe.executeGroup(group)
	}

	return pe.receipts
}

func (pe *ParallelExecutor) GetBoltContracts() map[string]agency.Contract {
	return pe.boltContractPool.Get().(map[string]agency.Contract)
}

func (pe *ParallelExecutor) AddNormalTx(hash *types.Hash) {
	pe.normalTxsMux.Lock()
	defer pe.normalTxsMux.Unlock()
	pe.normalTxs = append(pe.normalTxs, hash)
}

func (pe *ParallelExecutor) GetNormalTxs() []*types.Hash {
	return pe.normalTxs
}

func (pe *ParallelExecutor) AddInterchainCounter(to string, index *pb.VerifiedIndex) {
	pe.counterMux.Lock()
	defer pe.counterMux.Unlock()
	pe.interchainCounter[to] = append(pe.interchainCounter[to], index)
}

func (pe *ParallelExecutor) GetInterchainCounter() map[string][]*pb.VerifiedIndex {
	return pe.interchainCounter
}

func (pe *ParallelExecutor) executeGroup(group interface{}) {
	pe.logger.Infof("group type is %v", reflect.TypeOf(group))
	switch group.(type) {
	case *GroupNormal:
		normal := group.(*GroupNormal)
		for _, tx := range normal.txs {
			index := tx.GetIndex()
			receipt := pe.applyTxFunc(index, tx.GetTx(), nil)
			pe.receipts[index] = receipt
		}
		//pe.logger.Debugf("Nomral group has %d txs", len(normal.txs))

	case *GroupInterchain:
		interchain := group.(*GroupInterchain)
		wg := &sync.WaitGroup{}
		wg.Add(len(interchain.subGroups))

		// iterate thorough interchain txs and parallelizing between interchain groups
		for _, inter := range interchain.subGroups {
			go func(inter []*IndexedTx) {
				defer wg.Done()

				// set condition variable, current sequence and tx sequence in group into contract instance
				cont := pe.boltContractPool.Get().(map[string]agency.Contract)

				if runOnDAG {
					pe.logger.Infof("parallel executor interchain groups")
					pe.runWithDAG(inter, cont)
				} else {
					for _, vmTx := range inter {
						index := vmTx.GetIndex()
						receipt := pe.applyTxFunc(index, vmTx.GetTx(), &agency.TxOpt{
							Contracts: cont,
						})
						pe.receipts[index] = receipt
					}
				}

				// free contract to pool
				pe.boltContractPool.Put(cont)
			}(inter)
		}
		wg.Wait()
		pe.logger.Debugf("interchain group has %d subgroups", len(interchain.subGroups))
	default:
		panic("unknown group type")
	}
}
