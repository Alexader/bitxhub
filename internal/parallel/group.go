package parallel

import (
	"sync"

	"github.com/meshplus/bitxhub-model/pb"
)

type Group interface{}
type GroupType string

const (
	InterchainTx = "interchain"
	Normal       = "normal"
)

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

type XVMGroup struct {
	XvmTxs []*pb.Transaction
}

type BVMGroup struct {
	SubGroups []*BVMSubGroup
}

type BVMSubGroup struct {
	Type             GroupType
	normalGroup      []*pb.Transaction
	interchainGroups []*InterchainGroup
}

type InterchainGroup struct {
	Txs []*pb.Transaction
}

func (exec *ParallelBlockExecutor) executeXVMGroup(xvm *XVMGroup) []*pb.Receipt {
	return exec.applyTransactions(xvm.XvmTxs)
}

func (exec *ParallelBlockExecutor) executeBVMGroup(bvm *BVMGroup) []*pb.Receipt {
	// sequence execution in bvm group
	for _, sub := range bvm.SubGroups {
		exec.executeBVMSubGroup(sub)
	}
	return nil
}

func (exec *ParallelBlockExecutor) executeBVMSubGroup(sub *BVMSubGroup) []*pb.Receipt {
	if sub.Type == InterchainTx {
		// parallelizing validation engine part
		return exec.executeInterchainGroups(sub.interchainGroups)
	}

	// normal interchain related txs like appchain register and
	// rule register will be executed in line
	return exec.applyTransactions(sub.normalGroup)
}

func (exec *ParallelBlockExecutor) executeInterchainGroups(inters []*InterchainGroup) []*pb.Receipt {
	receipts := make([]*pb.Receipt, 0)
	wg := &sync.WaitGroup{}
	wg.Add(len(inters))

	// iterate thorough interchain txs and parallelizing between interchain groups
	for _, inter := range inters {
		go func(inter *InterchainGroup) {
			defer wg.Done()

			receipts = append(receipts, exec.executeInterchainGroup(inter)...)
		}(inter)
	}
	wg.Wait()
	return receipts
}

func (exec *ParallelBlockExecutor) executeInterchainGroup(inters *InterchainGroup) []*pb.Receipt {
	return nil
}
