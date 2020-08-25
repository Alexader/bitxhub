package parallel

import "github.com/meshplus/bitxhub-model/pb"

type XVMGroup struct {
	XvmTxs []*XVMTx
}

type BVMGroup struct {
	SubGroups []SubGroup
}

type SubGroup interface {
	Execute() []*pb.Receipt
}

type SubGroupNormal struct {
	exec        *ParallelBlockExecutor
	normalGroup []*BVMTx
}

type SubGroupInterchain struct {
	exec             *ParallelBlockExecutor
	interchainGroups [][]*BVMTx
}

func (subInterchain *SubGroupInterchain) genFromMap(m map[string][]*BVMTx) {
	subInterchain.interchainGroups = make([][]*BVMTx, 0, len(m))
	for _, interchainGroup := range m {
		subInterchain.interchainGroups =
			append(subInterchain.interchainGroups, interchainGroup)
	}
}

type VMTx interface {
	GetIndex() int
	GetTx() *pb.Transaction
}

type BVMTx struct {
	tx      *pb.Transaction
	txIndex int
	method  string
	args    []*pb.Arg
	isIBTP  bool
}

func (bvmTx *BVMTx) GetIndex() int {
	return bvmTx.txIndex
}

func (bvmTx *BVMTx) GetTx() *pb.Transaction {
	return bvmTx.tx
}

type XVMTx struct {
	tx      *pb.Transaction
	txIndex int
}

func (xvmTx *XVMTx) GetIndex() int {
	return xvmTx.txIndex
}

func (xvmTx *XVMTx) GetTx() *pb.Transaction {
	return xvmTx.tx
}
