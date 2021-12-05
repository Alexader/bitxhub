package executor

import (
	"github.com/meshplus/bitxhub-model/pb"
)

type IndexedTx struct {
	tx      *pb.Transaction
	txIndex int
}

func (pt *IndexedTx) GetIndex() int {
	return pt.txIndex
}

func (pt *IndexedTx) GetTx() *pb.Transaction {
	return pt.tx
}

type GroupNormal struct {
	txs []*IndexedTx
}

type GroupInterchain struct {
	subGroups [][]*IndexedTx
}

func groupTxs(txs []*pb.Transaction) ([]interface{}, error) {
	var groups []interface{}
	isPreviousInterchain := false
	subGroupNormal := &GroupNormal{}
	subGroupInterchain := &GroupInterchain{}
	normalGroup := make([]*IndexedTx, 0)
	interchainGroupM := make(map[string][]*IndexedTx, 0)

	for i, tx := range txs {
		pt := &IndexedTx{
			tx:      tx,
			txIndex: i,
		}
		if pt.tx.IsIBTP() {
			if !isPreviousInterchain {
				// if previous tx is non-interchain type,
				// add old subGroupNormal into subGroup of bvm and
				// create new subGroup for normal txs
				if len(normalGroup) != 0 {
					subGroupNormal.txs = normalGroup
					groups = append(groups, subGroupNormal)
					normalGroup = make([]*IndexedTx, 0)
					subGroupNormal = &GroupNormal{}
				}
			}

			isPreviousInterchain = true
			// classify interchainTx by its from-to addresses in IBTP struture
			from := tx.From.String()
			_, ok := interchainGroupM[from]
			if !ok {
				interchainGroupM[from] = make([]*IndexedTx, 0, 1)
			}
			interchainGroupM[from] = append(interchainGroupM[from], pt)

			continue
		}

		// for normal tx
		if isPreviousInterchain {
			// if previous tx is interchain type,
			// add old interchainGroups into subGroupInterchain and
			// create new subGroup for interchain
			if len(interchainGroupM) != 0 {
				subGroupInterchain.genFromMap(interchainGroupM)
				groups = append(groups, subGroupInterchain)
				subGroupInterchain = &GroupInterchain{}
				interchainGroupM = make(map[string][]*IndexedTx, 0)
			}
		}

		isPreviousInterchain = false
		normalGroup = append(normalGroup, pt)
	}

	// add last group into bvmGroup
	if len(normalGroup) != 0 {
		subGroupNormal.txs = normalGroup
		groups = append(groups, subGroupNormal)
	}

	if len(interchainGroupM) != 0 {
		subGroupInterchain.genFromMap(interchainGroupM)
		groups = append(groups, subGroupInterchain)
	}

	return groups, nil
}

func (group *GroupInterchain) genFromMap(m map[string][]*IndexedTx) {
	group.subGroups = make([][]*IndexedTx, 0, len(m))
	for _, interchainGroup := range m {
		group.subGroups =
			append(group.subGroups, interchainGroup)
	}
}
