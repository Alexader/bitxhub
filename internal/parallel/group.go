package parallel

import "github.com/meshplus/bitxhub-model/pb"

type TxGroup map[string][]*pb.Transaction

func (exec *ParallelBlockExecutor) groupTxs(txs []*pb.Transaction) (TxGroup, error) {
	// TODO: group txs implementation
	group := TxGroup{}
	return group, nil
}
