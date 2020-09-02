package contracts

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	appchainMgr "github.com/meshplus/bitxhub-core/appchain-mgr"
	"github.com/meshplus/bitxhub-kit/types"
	"github.com/meshplus/bitxhub-model/pb"
	"github.com/meshplus/bitxhub/internal/constant"
	"github.com/meshplus/bitxhub/pkg/vm/boltvm"
	"go.uber.org/atomic"
)

type ParallelInterchainManager struct {
	boltvm.Stub
	seq int32 // the sequence number of the interchain tx in group
	// to be executed on this contract
	curSeq *atomic.Int32 // the current sequence of executing interchain tx in its group
	cond   *sync.Cond    // the condition var to notify other waiting interchain tx instance
}

type ParallelInterchain struct {
	ID                   string            `json:"id"`
	InterchainCounter    map[string]uint64 `json:"interchain_counter,omitempty"`
	ReceiptCounter       map[string]uint64 `json:"receipt_counter,omitempty"`
	SourceReceiptCounter map[string]uint64 `json:"source_receipt_counter,omitempty"`
}

func (p *ParallelInterchain) UnmarshalJSON(data []byte) error {
	type alias Interchain
	t := &alias{}
	if err := json.Unmarshal(data, t); err != nil {
		return err
	}

	if t.InterchainCounter == nil {
		t.InterchainCounter = make(map[string]uint64)
	}

	if t.ReceiptCounter == nil {
		t.ReceiptCounter = make(map[string]uint64)
	}

	if t.SourceReceiptCounter == nil {
		t.SourceReceiptCounter = make(map[string]uint64)
	}

	*p = ParallelInterchain(*t)
	return nil
}

// SetCurrSequence is to set a shared atomic int32 var among all contracts
// whose interchain tx is in the same interchain group.
func (x *ParallelInterchainManager) SetCurrSequence(seq *atomic.Int32) {
	x.curSeq = seq
}

// args: cond, conditional variable for synchronizing between contracts
// SetCond will set a conditional var shared among all contracts whose interchain tx
// is in the same interchain group.
func (x *ParallelInterchainManager) SetCond(cond *sync.Cond) {
	x.cond = cond
}

// SetSequnce will set the sequence of interchain tx to executed on this contract
func (x *ParallelInterchainManager) SetSequnce(seq int32) {
	x.seq = seq
}

func (x *ParallelInterchainManager) Register() *boltvm.Response {
	interchain := &Interchain{ID: x.Caller()}
	ok := x.Has(x.appchainKey(x.Caller()))
	if ok {
		x.GetObject(x.appchainKey(x.Caller()), interchain)
	} else {
		x.SetObject(x.appchainKey(x.Caller()), interchain)
	}
	body, err := json.Marshal(interchain)
	if err != nil {
		return boltvm.Error(err.Error())
	}

	return boltvm.Success(body)
}

func (x *ParallelInterchainManager) DeleteInterchain(id string) *boltvm.Response {
	x.Delete(x.appchainKey(id))
	return boltvm.Success(nil)
}

// Interchain returns information of the interchain count, Receipt count and SourceReceipt count
func (x *ParallelInterchainManager) Interchain() *boltvm.Response {
	ok, data := x.Get(x.appchainKey(x.Caller()))
	if !ok {
		return boltvm.Error(fmt.Errorf("this appchain does not exist").Error())
	}
	return boltvm.Success(data)
}

func (x *ParallelInterchainManager) HandleIBTP(data []byte) *boltvm.Response {
	defer func() {
		// notify the process of next ibtp
		x.cond.L.Lock()
		defer x.cond.L.Unlock()

		// increase current sequence to advance other contract
		x.curSeq.Inc()
		x.cond.Broadcast()
	}()

	ok := x.Has(x.appchainKey(x.Caller()))
	if !ok {
		return boltvm.Error("this appchain does not exist")
	}

	ibtp := &pb.IBTP{}
	if err := ibtp.Unmarshal(data); err != nil {
		return boltvm.Error(err.Error())
	}

	if err := x.checkIBTP(ibtp); err != nil {
		return boltvm.Error(err.Error())
	}

	res := boltvm.Success(nil)

	x.cond.L.Lock()
	for x.curSeq.Load() != x.seq {
		x.cond.Wait()
	}
	x.cond.L.Unlock()

	//fmt.Printf("start interchain tx: %s\n", ibtp.ID())
	interchain := &ParallelInterchain{}
	x.GetObject(x.appchainKey(ibtp.From), &interchain)
	if err := x.checkIndex(ibtp, interchain); err != nil {
		//fmt.Printf("tx index wrong %s\n", err.Error())
		return boltvm.Error(err.Error())
	}

	if pb.IBTP_INTERCHAIN == ibtp.Type {
		//fmt.Println("invoke begin interchain tx")
		res = x.beginTransaction(ibtp)
	} else if pb.IBTP_RECEIPT_SUCCESS == ibtp.Type || pb.IBTP_RECEIPT_FAILURE == ibtp.Type {
		res = x.reportTransaction(ibtp)
	} else if pb.IBTP_ASSET_EXCHANGE_INIT == ibtp.Type ||
		pb.IBTP_ASSET_EXCHANGE_REDEEM == ibtp.Type ||
		pb.IBTP_ASSET_EXCHANGE_REFUND == ibtp.Type {
		res = x.handleAssetExchange(ibtp)
	}

	if !res.Ok {
		return res
	}

	x.ProcessIBTP(ibtp, interchain)
	return res
}

func (x *ParallelInterchainManager) HandleIBTPs(data []byte) *boltvm.Response {
	ok := x.Has(x.appchainKey(x.Caller()))
	if !ok {
		return boltvm.Error("this appchain does not exist")
	}

	ibtps := &pb.IBTPs{}
	if err := ibtps.Unmarshal(data); err != nil {
		return boltvm.Error(err.Error())
	}

	interchain := &ParallelInterchain{}
	x.GetObject(x.appchainKey(x.Caller()), &interchain)

	for _, ibtp := range ibtps.Iptp {
		if err := x.checkIBTP(ibtp); err != nil {
			return boltvm.Error(err.Error())
		}
	}

	if res := x.beginMultiTargetsTransaction(ibtps); !res.Ok {
		return res
	}

	for _, ibtp := range ibtps.Iptp {
		x.ProcessIBTP(ibtp, interchain)
	}

	return boltvm.Success(nil)
}

func (x *ParallelInterchainManager) checkIBTP(ibtp *pb.IBTP) error {
	if ibtp.To == "" {
		return fmt.Errorf("empty destination chain id")
	}
	if ok := x.Has(x.appchainKey(ibtp.To)); !ok {
		x.Logger().WithField("chain_id", ibtp.To).Warn("target appchain does not exist")
	}

	app := &appchainMgr.Appchain{}
	res := x.CrossInvoke(constant.AppchainMgrContractAddr.String(), "GetAppchain", pb.String(ibtp.From))
	if err := json.Unmarshal(res.Result, app); err != nil {
		return err
	}

	// get validation rule contract address
	res = x.CrossInvoke(constant.RuleManagerContractAddr.String(), "GetRuleAddress", pb.String(ibtp.From), pb.String(app.ChainType))
	if !res.Ok {
		return fmt.Errorf("this appchain does not register rule")
	}

	// handle validation
	isValid, err := x.ValidationEngine().Validate(string(res.Result), ibtp.From, ibtp.Proof, ibtp.Payload, app.Validators)
	if err != nil {
		return err
	}

	if !isValid {
		return fmt.Errorf("invalid interchain transaction")
	}

	return nil
}

func (x *ParallelInterchainManager) checkIndex(ibtp *pb.IBTP, interchain *ParallelInterchain) error {
	if pb.IBTP_INTERCHAIN == ibtp.Type ||
		pb.IBTP_ASSET_EXCHANGE_INIT == ibtp.Type ||
		pb.IBTP_ASSET_EXCHANGE_REDEEM == ibtp.Type ||
		pb.IBTP_ASSET_EXCHANGE_REFUND == ibtp.Type {
		if ibtp.From != x.Caller() {
			return fmt.Errorf("ibtp from != caller")
		}

		idx := interchain.InterchainCounter[ibtp.To]
		if idx+1 != ibtp.Index {
			return fmt.Errorf(fmt.Sprintf("wrong index, required %d, but %d", idx+1, ibtp.Index))
		}
	} else {
		if ibtp.To != x.Caller() {
			return fmt.Errorf("ibtp to != caller")
		}

		idx := interchain.ReceiptCounter[ibtp.To]
		if idx+1 != ibtp.Index {
			if interchain.SourceReceiptCounter[ibtp.To]+1 != ibtp.Index {
				return fmt.Errorf("wrong receipt index, required %d, but %d", idx+1, ibtp.Index)
			}
		}
	}
	return nil
}
func (x *ParallelInterchainManager) ProcessIBTP(ibtp *pb.IBTP, interchain *ParallelInterchain) {
	m := make(map[string]uint64)

	if pb.IBTP_INTERCHAIN == ibtp.Type ||
		pb.IBTP_ASSET_EXCHANGE_INIT == ibtp.Type ||
		pb.IBTP_ASSET_EXCHANGE_REDEEM == ibtp.Type ||
		pb.IBTP_ASSET_EXCHANGE_REFUND == ibtp.Type {
		interchain.InterchainCounter[ibtp.To]++
		x.SetObject(x.appchainKey(ibtp.From), interchain)
		x.SetObject(x.indexMapKey(ibtp.ID()), x.GetTxHash())
		m[ibtp.To] = x.GetTxIndex()
	} else {
		interchain.ReceiptCounter[ibtp.To] = ibtp.Index
		x.SetObject(x.appchainKey(ibtp.From), interchain)
		m[ibtp.From] = x.GetTxIndex()

		ic := &Interchain{}
		x.GetObject(x.appchainKey(ibtp.To), &ic)
		ic.SourceReceiptCounter[ibtp.From] = ibtp.Index
		x.SetObject(x.appchainKey(ibtp.To), ic)
	}

	x.PostInterchainEvent(m)
}

func (x *ParallelInterchainManager) beginMultiTargetsTransaction(ibtps *pb.IBTPs) *boltvm.Response {
	args := make([]*pb.Arg, 0)
	globalId := fmt.Sprintf("%s-%s", x.Caller(), x.GetTxHash())
	args = append(args, pb.String(globalId))

	for _, ibtp := range ibtps.Iptp {
		if ibtp.Type != pb.IBTP_INTERCHAIN {
			return boltvm.Error("ibtp type != IBTP_INTERCHAIN")
		}

		childTxId := fmt.Sprintf("%s-%s-%d", ibtp.From, ibtp.To, ibtp.Index)
		args = append(args, pb.String(childTxId))
	}

	return x.CrossInvoke(constant.TransactionMgrContractAddr.String(), "BeginMultiTXs", args...)
}

func (x *ParallelInterchainManager) beginTransaction(ibtp *pb.IBTP) *boltvm.Response {
	txId := fmt.Sprintf("%s-%s-%d", ibtp.From, ibtp.To, ibtp.Index)
	return x.CrossInvoke(constant.TransactionMgrContractAddr.String(), "Begin", pb.String(txId))
}

func (x *ParallelInterchainManager) reportTransaction(ibtp *pb.IBTP) *boltvm.Response {
	txId := fmt.Sprintf("%s-%s-%d", ibtp.From, ibtp.To, ibtp.Index)
	result := int32(0)
	if ibtp.Type == pb.IBTP_RECEIPT_FAILURE {
		result = 1
	}
	return x.CrossInvoke(constant.TransactionMgrContractAddr.String(), "Report", pb.String(txId), pb.Int32(result))
}

func (x *ParallelInterchainManager) handleAssetExchange(ibtp *pb.IBTP) *boltvm.Response {
	var method string

	switch ibtp.Type {
	case pb.IBTP_ASSET_EXCHANGE_INIT:
		method = "Init"
	case pb.IBTP_ASSET_EXCHANGE_REDEEM:
		method = "Redeem"
	case pb.IBTP_ASSET_EXCHANGE_REFUND:
		method = "Refund"
	default:
		return boltvm.Error("unsupported asset exchange type")
	}

	return x.CrossInvoke(constant.AssetExchangeContractAddr.String(), method, pb.String(ibtp.From),
		pb.String(ibtp.To), pb.Bytes(ibtp.Extra))
}

func (x *ParallelInterchainManager) GetIBTPByID(id string) *boltvm.Response {
	arr := strings.Split(id, "-")
	if len(arr) != 3 {
		return boltvm.Error("wrong ibtp id")
	}

	caller := x.Caller()

	if caller != arr[0] && caller != arr[1] {
		return boltvm.Error("The caller does not have access to this ibtp")
	}

	var hash types.Hash
	exist := x.GetObject(x.indexMapKey(id), &hash)
	if !exist {
		return boltvm.Error("this id is not existed")
	}

	return boltvm.Success(hash.Bytes())
}

func (x *ParallelInterchainManager) appchainKey(id string) string {
	return appchainMgr.PREFIX + id
}

func (x *ParallelInterchainManager) indexMapKey(id string) string {
	return fmt.Sprintf("index-tx-%s", id)
}
