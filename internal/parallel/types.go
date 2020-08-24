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

type VMTx interface {
	GetIndex() int
	GetTx() *pb.Transaction
}

type BVMTx struct {
	tx      *pb.Transaction
	txIndex int
	method  string
	args    []byte
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
