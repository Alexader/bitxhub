package parallel

import (
	"sync"

	"github.com/meshplus/bitxhub-model/pb"
)

type Group interface{}

func (exec *ParallelBlockExecutor) groupTxs(txs []*pb.Transaction) ([]Group, error) {
	// TODO: group interchainGroups implementation
	xvmGroup := &XVMGroup{}
	bvmGroup := &BVMGroup{}

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
		receipts = append(receipts, exec.applyVMTx(tx))
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
		receipts = append(receipts, normal.exec.applyVMTx(tx))
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
			receipts = append(receipts, interchain.executeInterchainGroup(inter)...)
		}(inter)
	}
	wg.Wait()
	return receipts
}

func (interchain *SubGroupInterchain) executeInterchainGroup(txs []*BVMTx) []*pb.Receipt {
	// parallelizing validation engine part
	return nil
}
