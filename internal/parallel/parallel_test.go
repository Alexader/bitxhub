package parallel

import (
	"encoding/json"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/meshplus/bitxhub-kit/crypto/asym/ecdsa"
	"github.com/meshplus/bitxhub-kit/log"
	"github.com/meshplus/bitxhub-kit/types"
	"github.com/meshplus/bitxhub-model/pb"
	"github.com/meshplus/bitxhub/internal/constant"
	"github.com/meshplus/bitxhub/internal/ledger"
	"github.com/meshplus/bitxhub/internal/ledger/mock_ledger"
	"github.com/stretchr/testify/assert"
)

const (
	appchainA = "0x3f9d18f7c3a6e5e4c0b877fe3e688ab08840b997"
	appchainB = "0xa8ae1bbc1105944a84a71b89056930d951d420fe"
	appchainC = "0x929545f44692178edb7fa468b44c5351596184ba"
	appchainD = "0x7368022e6659236983eb959b8a1fa22577d48294"
)

func TestGroup(t *testing.T) {
	mockCtl := gomock.NewController(t)
	mockLedger := mock_ledger.NewMockLedger(mockCtl)

	chainMeta := &pb.ChainMeta{
		Height:    1,
		BlockHash: types.String2Hash(from),
	}
	evs := make([]*pb.Event, 0)
	m := make(map[string]uint64)
	m[from] = 3
	data, err := json.Marshal(m)
	assert.Nil(t, err)
	ev := &pb.Event{
		TxHash:     types.String2Hash(from),
		Data:       data,
		Interchain: true,
	}
	evs = append(evs, ev)
	mockLedger.EXPECT().GetChainMeta().Return(chainMeta).AnyTimes()
	mockLedger.EXPECT().GetBalance(gomock.Any()).Return(uint64(100)).AnyTimes()
	mockLedger.EXPECT().Events(gomock.Any()).Return(evs).AnyTimes()
	mockLedger.EXPECT().Commit(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockLedger.EXPECT().Clear().AnyTimes()
	mockLedger.EXPECT().GetState(gomock.Any(), gomock.Any()).Return(true, []byte("10")).AnyTimes()
	mockLedger.EXPECT().SetState(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	mockLedger.EXPECT().GetBalance(gomock.Any()).Return(uint64(10)).AnyTimes()
	mockLedger.EXPECT().SetBalance(gomock.Any(), gomock.Any()).AnyTimes()
	mockLedger.EXPECT().SetNonce(gomock.Any(), gomock.Any()).AnyTimes()
	mockLedger.EXPECT().GetNonce(gomock.Any()).Return(uint64(0)).AnyTimes()
	mockLedger.EXPECT().SetCode(gomock.Any(), gomock.Any()).AnyTimes()
	mockLedger.EXPECT().GetCode(gomock.Any()).Return([]byte("10")).AnyTimes()
	mockLedger.EXPECT().PersistExecutionResult(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockLedger.EXPECT().FlushDirtyDataAndComputeJournal().Return(make(map[string]*ledger.Account), &ledger.BlockJournal{}).AnyTimes()
	mockLedger.EXPECT().PersistBlockData(gomock.Any()).AnyTimes()
	logger := log.NewWithModule("executor")

	exec, err := New(mockLedger, logger)
	assert.Nil(t, err)

	var txs []*pb.Transaction
	privKey, err := ecdsa.GenerateKey(ecdsa.Secp256r1)
	assert.Nil(t, err)
	pubKey := privKey.PublicKey()
	addr, err := pubKey.Address()
	assert.Nil(t, err)
	from = addr.String()

	// set tx of TransactionData_BVM type
	bvmTypes := []constant.BoltContractAddress{constant.AppchainMgrContractAddr, constant.AppchainMgrContractAddr}
	for i := uint64(0); i < 2; i++ {
		BVMTx := mockNormalTx(t, bvmTypes[i])
		txs = append(txs, BVMTx)
	}

	appchains := []string{appchainA, appchainB, appchainB, appchainA}
	for i := uint64(0); i < 4; i++ {
		ibtp := mockInterchainIBTP(t, i+1, pb.IBTP_INTERCHAIN, appchains[i])
		BVMData := mockInterchainTxData(t, ibtp)
		BVMTx := mockInterchainTx(BVMData)
		txs = append(txs, BVMTx)
	}

	// set tx of TransactionData_XVM type
	XVMTx := mockXVMTx(t)
	txs = append(txs, XVMTx)
	// set tx of TransactionData_NORMAL type
	NormalTx := mockXVMTx(t)
	txs = append(txs, NormalTx)

	appchains = []string{appchainC, appchainD, appchainC, appchainA}
	for i := uint64(0); i < 4; i++ {
		ibtp := mockInterchainIBTP(t, i+3, pb.IBTP_INTERCHAIN, appchains[i])
		BVMData := mockInterchainTxData(t, ibtp)
		BVMTx := mockInterchainTx(BVMData)
		txs = append(txs, BVMTx)
	}

	// add one asset_exchange tx
	assetExchangeTx := mockAssetExchangeTx(t)
	txs = append(txs, assetExchangeTx)

	// set signature for txs
	for _, tx := range txs {
		tx.Timestamp = time.Now().UnixNano()
		tx.Nonce = rand.Int63()
		tx.From, err = pubKey.Address()
		assert.Nil(t, err)
		sig, err := privKey.Sign(tx.SignHash().Bytes())
		tx.Signature = sig
		assert.Nil(t, err)
		tx.TransactionHash = tx.Hash()
	}

	g, err := exec.groupTxs(txs)
	assert.Nil(t, err)

	assert.NotNil(t, g)
	assert.Equal(t, 2, len(g))
	xvmGroup, ok := g[0].(*XVMGroup)
	assert.True(t, ok)

	bvmGroup, ok := g[1].(*BVMGroup)
	assert.True(t, ok)

	// 2 xvm tx in xvm group
	assert.Equal(t, 2, len(xvmGroup.XvmTxs))

	// 3 sub group for bvm group
	assert.Equal(t, 3, len(bvmGroup.SubGroups))
	normalGroup, ok := bvmGroup.SubGroups[0].(*SubGroupNormal)
	assert.True(t, ok)
	assert.Equal(t, 2, len(normalGroup.normalGroup))

	// 8 handleIBTP tx in second group of the bvm group
	interchainGroup, ok := bvmGroup.SubGroups[1].(*SubGroupInterchain)
	assert.True(t, ok)
	assert.Equal(t, 4, len(interchainGroup.interchainGroups))
	groupLens := [4]int{
		len(interchainGroup.interchainGroups[0]),
		len(interchainGroup.interchainGroups[1]),
		len(interchainGroup.interchainGroups[2]),
		len(interchainGroup.interchainGroups[3]),
	}

	sort.Ints(groupLens[:])
	assert.Equal(t, [4]int{1, 2, 2, 3}, groupLens)

	assetGroup, ok := bvmGroup.SubGroups[2].(*SubGroupNormal)
	assert.True(t, ok)
	assert.Equal(t, 1, len(assetGroup.normalGroup))
	assert.True(t, !assetGroup.normalGroup[0].isIBTP)
	assert.Equal(t, assetExchangeTx, assetGroup.normalGroup[0].tx)

	// process mock block
	block := mockBlock(uint64(2), txs)
	exec.processExecuteEvent(block)

	blockData := <-exec.persistC
	rs := blockData.Receipts
	assert.Equal(t, len(txs), len(rs))
	for i, r := range rs {
		assert.Equal(t, txs[i].TransactionHash.Hex(), r.TxHash.Hex())
	}
}

func mockInterchainTxData(t *testing.T, ibtp *pb.IBTP) *pb.TransactionData {
	arg, err := ibtp.Marshal()
	assert.Nil(t, err)

	tmpIP := &pb.InvokePayload{
		Method: "HandleIBTP",
		Args:   []*pb.Arg{pb.Bytes(arg)},
	}
	pd, err := tmpIP.Marshal()
	assert.Nil(t, err)

	return &pb.TransactionData{
		VmType:  pb.TransactionData_BVM,
		Type:    pb.TransactionData_INVOKE,
		Payload: pd,
	}
}

func registerPayload() *pb.InvokePayload {
	return &pb.InvokePayload{
		Method: "Register",
		Args: []*pb.Arg{
			pb.String("validator"),
			pb.Int32(1), pb.String("fabric"),
			pb.String("fab"), pb.String("fabric for law"),
			pb.String("1.4.3"), pb.String(""),
		},
	}
}

func rulePayload() *pb.InvokePayload {
	return &pb.InvokePayload{
		Method: "RegisterRule",
		Args: []*pb.Arg{
			pb.String("validator"),
			pb.Int32(1), pb.String("fabric"),
		},
	}
}

func mockNormalTx(t *testing.T, boltAddr constant.BoltContractAddress) *pb.Transaction {
	var pl *pb.InvokePayload
	switch boltAddr {
	case constant.AppchainMgrContractAddr:
		pl = registerPayload()
	case constant.RuleManagerContractAddr:
		pl = rulePayload()
	}
	pd, err := pl.Marshal()
	assert.Nil(t, err)

	data := &pb.TransactionData{
		VmType:  pb.TransactionData_BVM,
		Type:    pb.TransactionData_INVOKE,
		Payload: pd,
	}

	return &pb.Transaction{
		From:  types.String2Address(from),
		To:    types.String2Address(boltAddr.String()),
		Data:  data,
		Nonce: rand.Int63(),
	}
}

func mockInterchainTx(data *pb.TransactionData) *pb.Transaction {
	return &pb.Transaction{
		To:   types.String2Address(constant.ParallelInterchainContractAddr.String()),
		Data: data,
	}
}

func mockAssetExchangeTx(t *testing.T) *pb.Transaction {
	appchains := []string{appchainA, appchainB, appchainD}
	assetExchangeIBTPs := &pb.IBTPs{Iptp: make([]*pb.IBTP, 3)}
	for i := uint64(0); i < 3; i++ {
		assetExchangeIBTPs.Iptp[i] = mockInterchainIBTP(t, i+1, pb.IBTP_INTERCHAIN, appchains[i])
	}
	arg, err := assetExchangeIBTPs.Marshal()
	assert.Nil(t, err)

	tmpIP := &pb.InvokePayload{
		Method: "HandleIBTPs",
		Args:   []*pb.Arg{{Value: arg}},
	}
	pd, err := tmpIP.Marshal()
	assert.Nil(t, err)

	data := &pb.TransactionData{
		VmType:  pb.TransactionData_BVM,
		Type:    pb.TransactionData_INVOKE,
		Payload: pd,
	}

	return &pb.Transaction{
		To:   types.String2Address(constant.ParallelInterchainContractAddr.String()),
		Data: data,
	}
}

func mockXVMTx(t *testing.T) *pb.Transaction {
	tmpIP := &pb.InvokePayload{
		Method: "set",
		Args:   []*pb.Arg{{Value: []byte("Alice,100")}},
	}
	pd, err := tmpIP.Marshal()
	assert.Nil(t, err)

	data := &pb.TransactionData{
		VmType:  pb.TransactionData_XVM,
		Type:    pb.TransactionData_NORMAL,
		Amount:  10,
		Payload: pd,
	}

	return &pb.Transaction{
		To:   types.String2Address(constant.ParallelInterchainContractAddr.String()),
		Data: data,
	}
}

func mockInterchainIBTP(t *testing.T, index uint64, typ pb.IBTP_Type, dstChain string) *pb.IBTP {
	content := &pb.Content{
		SrcContractId: from,
		DstContractId: dstChain,
		Func:          "interchainCharge",
		Args:          [][]byte{[]byte(from + ",1,Alice,Bob,1")},
	}

	bytes, err := content.Marshal()
	assert.Nil(t, err)

	ibtppd, err := json.Marshal(&pb.Payload{
		Encrypted: false,
		Content:   bytes,
	})
	assert.Nil(t, err)

	return &pb.IBTP{
		From:      from,
		To:        dstChain,
		Payload:   ibtppd,
		Index:     index,
		Type:      typ,
		Timestamp: time.Now().UnixNano(),
	}
}
