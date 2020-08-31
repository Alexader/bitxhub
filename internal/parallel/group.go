package parallel

import (
	"sync"

	"github.com/meshplus/bitxhub-model/pb"
	"github.com/meshplus/bitxhub/internal/constant"
	"github.com/meshplus/bitxhub/internal/executor/contracts"
	"go.uber.org/atomic"
)

const (
	handleIBTP = "HandleIBTP"
)

type Group interface{}

func (exec *ParallelBlockExecutor) classifyXVM(txs []*pb.Transaction) ([]*XVMTx, []*BVMTx) {
	xvmTxs := make([]*XVMTx, 0)
	bvmTxs := make([]*BVMTx, 0)

	for i, tx := range txs {
		if tx.Data == nil {
			//return nil, nil, fmt.Errorf("empty transaction data")
			continue
		}

		switch tx.Data.Type {
		case pb.TransactionData_NORMAL:
			xvmTxs = append(xvmTxs, &XVMTx{
				tx:      tx,
				txIndex: i,
			})
		default:
			switch tx.Data.VmType {
			case pb.TransactionData_BVM:
				payload := &pb.InvokePayload{}
				if err := payload.Unmarshal(tx.Data.Payload); err != nil {
					continue
				}

				bvm := &BVMTx{
					tx:      tx,
					isIBTP:  false,
					method:  payload.Method,
					args:    payload.Args,
					txIndex: i,
				}
				bvm.isIBTP = payload.Method == handleIBTP &&
					tx.To.Hex() == constant.ParallelInterchainContractAddr.String()

				bvmTxs = append(bvmTxs, bvm)
			case pb.TransactionData_XVM:
				xvmTxs = append(xvmTxs, &XVMTx{
					tx:      tx,
					txIndex: i,
				})
			}
		}
	}
	return xvmTxs, bvmTxs
}

func (exec *ParallelBlockExecutor) classifyBVM(bvmTxs []*BVMTx) *BVMGroup {
	bvmGroup := &BVMGroup{SubGroups: make([]SubGroup, 0)}
	isPreviousInterchain := false
	subGroupNormal := &SubGroupNormal{exec: exec}
	subGroupInterchain := &SubGroupInterchain{exec: exec}
	normalGroup := make([]*BVMTx, 0)
	interchainGroupM := make(map[string][]*BVMTx, 0)

	for _, bvmTx := range bvmTxs {
		if bvmTx.isIBTP {
			if !isPreviousInterchain {
				// if previous tx is non-interchain type,
				// add old subGroupNormal into subGroup of bvm and
				// create new subGroup for normal txs
				if len(normalGroup) != 0 {
					subGroupNormal.normalGroup = normalGroup
					bvmGroup.SubGroups = append(bvmGroup.SubGroups, subGroupNormal)
					normalGroup = make([]*BVMTx, 0)
					subGroupNormal = &SubGroupNormal{exec: exec}
				}
			}

			isPreviousInterchain = true
			// classify interchainTx by its from-to addresses in IBTP struture
			switch bvmTx.method {
			case handleIBTP:
				ibtp := &pb.IBTP{}
				if err := ibtp.Unmarshal(bvmTx.args[0].Value); err != nil {
					continue
				}

				_, ok := interchainGroupM[ibtp.To]
				if !ok {
					interchainGroupM[ibtp.To] = make([]*BVMTx, 0, 1)
				}
				interchainGroupM[ibtp.To] = append(interchainGroupM[ibtp.To], bvmTx)
			}
			continue
		}

		// for normal tx
		if isPreviousInterchain {
			// if previous tx is interchain type,
			// add old interchainGroups into subGroupInterchain and
			// create new subGroup for interchain
			if len(interchainGroupM) != 0 {
				subGroupInterchain.genFromMap(interchainGroupM)
				bvmGroup.SubGroups = append(bvmGroup.SubGroups, subGroupInterchain)
				subGroupInterchain = &SubGroupInterchain{exec: exec}
				interchainGroupM = make(map[string][]*BVMTx, 0)
			}
		}

		isPreviousInterchain = false
		normalGroup = append(normalGroup, bvmTx)
	}

	// add last group into bvmGroup
	if len(normalGroup) != 0 {
		subGroupNormal.normalGroup = normalGroup
		bvmGroup.SubGroups = append(bvmGroup.SubGroups, subGroupNormal)
	}

	if len(interchainGroupM) != 0 {
		subGroupInterchain.genFromMap(interchainGroupM)
		bvmGroup.SubGroups = append(bvmGroup.SubGroups, subGroupInterchain)
	}

	return bvmGroup
}

func (exec *ParallelBlockExecutor) groupTxs(txs []*pb.Transaction) ([]Group, error) {
	xvmGroup := &XVMGroup{}
	// first round, group into xvm and bvm
	xvmTxs, bvmTxs := exec.classifyXVM(txs)

	// add xvm txs into xvmGroup
	xvmGroup.XvmTxs = xvmTxs

	// second round, group bvm into sub groups
	bvmGroup := exec.classifyBVM(bvmTxs)

	return []Group{xvmGroup, bvmGroup}, nil
}

func (exec *ParallelBlockExecutor) applyGroup(g Group) []*pb.Receipt {
	switch group := g.(type) {
	case *BVMGroup:
		return exec.executeBVMGroup(group)
	case *XVMGroup:
		return exec.executeXVMGroup(group)
	default:
		panic("wrong type of group")
	}
}

func (exec *ParallelBlockExecutor) executeXVMGroup(xvm *XVMGroup) []*pb.Receipt {
	receipts := make([]*pb.Receipt, 0, len(xvm.XvmTxs))
	for _, tx := range xvm.XvmTxs {
		receipt := &pb.Receipt{
			Version: tx.GetTx().Version,
			TxHash:  tx.GetTx().TransactionHash,
		}
		exec.applyVMTx(tx, receipt, nil)
		receipts = append(receipts, receipt)
	}
	return receipts
}

func (exec *ParallelBlockExecutor) executeBVMGroup(bvm *BVMGroup) []*pb.Receipt {
	// sequence execution in bvm group
	receipts := make([]*pb.Receipt, 0)

	for _, sub := range bvm.SubGroups {
		rs := sub.Execute()
		receipts = append(receipts, rs...)
	}
	return receipts
}

// normal-tx group including txs like appchain register and
// rule register tx, which will be executed in line
func (normal *SubGroupNormal) Execute() []*pb.Receipt {
	receipts := make([]*pb.Receipt, 0, len(normal.normalGroup))
	for _, tx := range normal.normalGroup {
		receipt := &pb.Receipt{
			Version: tx.GetTx().Version,
			TxHash:  tx.GetTx().TransactionHash,
		}
		normal.exec.applyVMTx(tx, receipt, nil)
		receipts = append(receipts, receipt)
	}
	return receipts
}

// interchain-tx will have sub groups and can be parallelized
func (interchain *SubGroupInterchain) Execute() []*pb.Receipt {
	receipts := make([]*pb.Receipt, 0)
	wg := &sync.WaitGroup{}
	mux := sync.Mutex{}
	wg.Add(len(interchain.interchainGroups))

	// iterate thorough interchain txs and parallelizing between interchain groups
	for _, inter := range interchain.interchainGroups {
		go func(inter []*BVMTx) {
			defer wg.Done()

			mux.Lock()
			defer mux.Unlock()
			receipts = append(receipts, interchain.exec.executeInterchainGroup(inter)...)
		}(inter)
	}
	wg.Wait()
	return receipts
}

func (exec *ParallelBlockExecutor) executeInterchainGroup(txs []*BVMTx) []*pb.Receipt {
	// parallelizing validation engine part
	receipts := make([]*pb.Receipt, 0, len(txs))
	mux := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(txs))

	lock := sync.Mutex{}
	cond := sync.NewCond(&lock)
	currSeq := atomic.NewInt32(0)

	for i, bvmTx := range txs {
		// interchain txs inside a group can be parallelized in validation engine
		go func(i int, vmTx *BVMTx) {
			defer wg.Done()

			receipt := &pb.Receipt{
				Version: vmTx.GetTx().Version,
				TxHash:  vmTx.GetTx().TransactionHash,
			}

			// set condition variable, current sequence and tx sequence in group into contract instance
			cont := exec.interchainContractPool.Get().(*contracts.ParallelInterchainManager)
			cont.SetCond(cond)
			cont.SetCurrSequence(currSeq)
			cont.SetSequnce(int32(i))

			exec.applyVMTx(vmTx, receipt, &TxOpt{
				Contract: cont,
			})

			mux.Lock()
			defer mux.Unlock()
			receipts = append(receipts, receipt)

			// free contract to pool
			exec.interchainContractPool.Put(cont)
		}(i, bvmTx)
	}

	wg.Wait()
	return receipts
}
