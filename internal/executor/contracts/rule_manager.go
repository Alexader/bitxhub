package contracts

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	appchainMgr "github.com/meshplus/bitxhub-core/appchain-mgr"
	"github.com/meshplus/bitxhub-core/boltvm"
	"github.com/meshplus/bitxhub-core/governance"
	ruleMgr "github.com/meshplus/bitxhub-core/rule-mgr"
	"github.com/meshplus/bitxhub-core/validator"
	"github.com/meshplus/bitxhub-model/constant"
	"github.com/meshplus/bitxhub-model/pb"
	"github.com/meshplus/bitxid"
	"github.com/meshplus/eth-kit/ledger"
	"github.com/tidwall/gjson"
)

// RuleManager is the contract manage validation rules
type RuleManager struct {
	boltvm.Stub
	ruleMgr.RuleManager
}

type UpdateMasterRuleInfo struct {
	NewRule  *ruleMgr.Rule         `json:"new_rule"`
	Appchain *appchainMgr.Appchain `json:"appchain"`
}

// extra: ruleMgr.rule
func (rm *RuleManager) Manage(eventTyp string, proposalResult, lastStatus string, extra []byte) *boltvm.Response {
	specificAddrs := []string{constant.GovernanceContractAddr.Address().String()}
	addrsData, err := json.Marshal(specificAddrs)
	if err != nil {
		return boltvm.Error("marshal specificAddrs error:" + err.Error())
	}
	res := rm.CrossInvoke(constant.RoleContractAddr.String(), "CheckPermission",
		pb.String(string(PermissionSpecific)),
		pb.String(""),
		pb.String(rm.CurrentCaller()),
		pb.Bytes(addrsData))
	if !res.Ok {
		return boltvm.Error("check permission error:" + string(res.Result))
	}

	rm.RuleManager.Persister = rm.Stub
	info := &UpdateMasterRuleInfo{}
	if err := json.Unmarshal(extra, &info); err != nil {
		return boltvm.Error("unmarshal info error:" + err.Error())
	}

	switch eventTyp {
	case string(governance.EventUpdate):
		// get master
		masterRule := &ruleMgr.Rule{}
		ok, masterData := rm.RuleManager.GetMaster(info.NewRule.ChainId)
		if !ok {
			return boltvm.Error("get master error: " + string(masterData))
		}
		if err := json.Unmarshal(masterData, &masterRule); err != nil {
			return boltvm.Error("unmarshal masterRule error:" + err.Error())
		}
		// change master status
		ok, errData := rm.RuleManager.ChangeStatus(masterRule.Address, proposalResult, string(governance.GovernanceAvailable), []byte(masterRule.ChainId))
		if !ok {
			return boltvm.Error(string(errData))
		}

		// If the update succeeds, restore the status of the appchain
		if proposalResult == string(APPROVED) {
			res := rm.CrossInvoke(constant.AppchainMgrContractAddr.Address().String(), "UnPauseAppchain", pb.String(info.Appchain.ID), pb.String(string(info.Appchain.Status)))
			if !res.Ok {
				return boltvm.Error(fmt.Sprintf("cross invoke UnPauseAppchain err: %s", res.Result))
			}
		}
	}

	ok, errData := rm.RuleManager.ChangeStatus(info.NewRule.Address, proposalResult, lastStatus, []byte(info.NewRule.ChainId))
	if !ok {
		return boltvm.Error(string(errData))
	}

	return boltvm.Success(nil)
}

// Register records the rule, and then automatically binds the rule if there is no master validation rule
func (rm *RuleManager) RegisterRule(chainId string, ruleAddress, ruleUrl, reason string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub

	// 1. check permission
	if err := rm.checkPermission(chainId, PermissionSelfAdmin, nil); err != nil {
		return boltvm.Error(err.Error())
	}

	// 2. check appchain
	if res := rm.CrossInvoke(constant.AppchainMgrContractAddr.String(), "IsAvailable", pb.String(chainId)); !res.Ok {
		return boltvm.Error("cross invoke IsAvailable error: " + string(res.Result))
	}

	// 3. check rule
	if err := rm.checkRuleAddress(ruleAddress); err != nil {
		return boltvm.Error(err.Error())
	}

	// 4. register
	ok, data := rm.RuleManager.Register(chainId, ruleAddress, ruleUrl)
	if !ok {
		return boltvm.Error("register error: " + string(data))
	}

	registerRes := &governance.RegisterResult{}
	if err := json.Unmarshal(data, registerRes); err != nil {
		return boltvm.Error("unmarshal error: " + err.Error())
	}
	if registerRes.IsRegistered {
		return boltvm.Error("rule has registered, chain id: " + chainId + ", rule addr: " + ruleAddress)
	}

	// 5. determine whether to bind, if not bind:
	// -  existing master validation rules
	// -  there is already rule in binding
	if ok, _ := rm.RuleManager.BindPre(chainId, ruleAddress, false); !ok {
		return getGovernanceRet("", nil)
	}

	// 6. submit proposal
	// 7. change status
	return rm.bindRule(&UpdateMasterRuleInfo{
		NewRule: &ruleMgr.Rule{
			Address: ruleAddress,
			ChainId: chainId,
			Status:  governance.GovernanceUnavailable,
		},
	}, governance.EventBind, reason)
}

// Register records the rule, and then automatically binds the rule if there is no master validation rule
func (rm *RuleManager) RegisterRuleV2(chainId, ruleAddress, ruleUrl string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub

	method := bitxid.DID(chainId).GetSubMethod()
	appchainAdmin := strings.TrimPrefix(method, "appchain")

	if rm.Caller() != appchainAdmin {
		return boltvm.Error(fmt.Sprintf("caller %s is not appchain admin %s", rm.Caller(), appchainAdmin))
	}

	// 1. check rule
	if err := rm.checkRuleAddress(ruleAddress); err != nil {
		return boltvm.Error(err.Error())
	}

	// 2. register
	ok, data := rm.RuleManager.Register(chainId, ruleAddress, ruleUrl)
	if !ok {
		return boltvm.Error("register error: " + string(data))
	}

	registerRes := &governance.RegisterResult{}
	if err := json.Unmarshal(data, registerRes); err != nil {
		return boltvm.Error("unmarshal error: " + err.Error())
	}
	if registerRes.IsRegistered {
		return boltvm.Error("rule has registered, chain id: " + chainId + ", rule addr: " + ruleAddress)
	}

	return boltvm.Success(nil)
}

// DefaultRule automatically adds default rules to the appchain after the appchain is registered successfully
// DefaultRule Adds default rules automatically. The rule will automatically bound if there is no master rule currently. All processes do not require vote.
// Possible situations:
// - Default validation rules are automatically added after successful application chain registration
// - It may be necessary to restore the identity of the master rule if it fails to freeze or logout
func (rm *RuleManager) DefaultRule(chainId string, ruleAddress string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub

	// 1. check permission
	specificAddrs := []string{constant.AppchainMgrContractAddr.Address().String(), constant.GovernanceContractAddr.Address().String()}
	addrsData, err := json.Marshal(specificAddrs)
	if err != nil {
		return boltvm.Error("marshal specificAddrs error:" + err.Error())
	}
	if err := rm.checkPermission(chainId, PermissionSpecific, addrsData); err != nil {
		return boltvm.Error(err.Error())
	}

	// 2. register
	ok, data := rm.RuleManager.Register(chainId, ruleAddress, "")
	if !ok {
		return boltvm.Error("register error: " + string(data))
	}

	// 3. default bind
	ok, data = rm.RuleManager.CountAvailable([]byte(chainId))
	if !ok {
		return boltvm.Error("count available error: " + string(data))
	}
	if string(data) == strconv.Itoa(0) {
		ok, data = rm.RuleManager.ChangeStatus(ruleAddress, string(governance.EventBind), string(governance.GovernanceBindable), []byte(chainId))
		if !ok {
			return boltvm.Error("change status error: " + string(data))
		}
		ok, data = rm.RuleManager.ChangeStatus(ruleAddress, string(governance.EventApprove), string(governance.GovernanceBindable), []byte(chainId))
		if !ok {
			return boltvm.Error("change status error: " + string(data))
		}
	}

	return boltvm.Success(nil)
}

// BindRule binds the validation rule address with the chain id
func (rm *RuleManager) UpdateMasterRule(chainId string, newMasterruleAddress, reason string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub

	// 1. check permission
	if err := rm.checkPermission(chainId, PermissionSelfAdmin, nil); err != nil {
		return boltvm.Error(err.Error())
	}

	// 2. check appchain
	res := rm.CrossInvoke(constant.AppchainMgrContractAddr.Address().String(), "PauseAppchain", pb.String(chainId))
	if !res.Ok {
		return boltvm.Error(fmt.Sprintf("cross invoke PauseAppchain error: %s", string(res.Result)))
	}
	appchain := &appchainMgr.Appchain{}
	if err := json.Unmarshal(res.Result, appchain); err != nil {
		return boltvm.Error(fmt.Sprintf("unmarshal appchain error: %v", err))
	}

	// 3. check new rule
	if err := rm.checkRuleAddress(newMasterruleAddress); err != nil {
		return boltvm.Error(err.Error())
	}

	// 4. new rule pre bind
	ok, data := rm.RuleManager.BindPre(chainId, newMasterruleAddress, true)
	if !ok {
		return boltvm.Error("bind prepare error: " + string(data))
	}

	// 5. submit new rule bind proposal
	// 6. change new rule status
	newRule := &ruleMgr.Rule{}
	if err := json.Unmarshal(data, newRule); err != nil {
		return boltvm.Error(fmt.Sprintf("unmarshal rule err: %v", err))
	}
	info := &UpdateMasterRuleInfo{
		NewRule:  newRule,
		Appchain: appchain,
	}
	res = rm.bindRule(info, governance.EventUpdate, reason)
	if !res.Ok {
		return res
	}

	// 7. operate master rule
	ok, masterData := rm.RuleManager.GetMaster(chainId)
	if !ok {
		return boltvm.Error("get master error: " + string(masterData))
	}
	masterRuleAddress := gjson.Get(string(masterData), "address").String()
	if ok, data := rm.RuleManager.ChangeStatus(masterRuleAddress, string(governance.EventUnbind), string(governance.GovernanceAvailable), []byte(chainId)); !ok {
		return boltvm.Error("change status error: " + string(data))
	}
	return res
}

func (rm *RuleManager) BindRule(chainId string, ruleAddr, ruleUrl string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub

	if rm.CurrentCaller() != constant.AppchainMgrContractAddr.Address().String() {
		return boltvm.Error(fmt.Sprintf("current caller %s is not appchain manager contract", rm.Caller()))
	}

	if ruleAddr == validator.FabricRuleAddr || ruleAddr == validator.SimFabricRuleAddr || ruleAddr == validator.HappyRuleAddr {
		ok, data := rm.RuleManager.Register(chainId, ruleAddr, ruleUrl)
		if !ok {
			return boltvm.Error("register error: " + string(data))
		}
	}

	ruleRes := rm.GetRuleByAddr(chainId, ruleAddr)
	if !ruleRes.Ok {
		return boltvm.Error("get rule by addr error: " + string(ruleRes.Result))
	}
	rule := &ruleMgr.Rule{}
	if err := json.Unmarshal(ruleRes.Result, &rule); err != nil {
		return boltvm.Error("unmarshal rule error:" + err.Error())
	}

	// change status
	if ok, data := rm.RuleManager.ChangeStatus(ruleAddr, string(governance.EventBind), string(rule.Status), []byte(chainId)); !ok {
		return boltvm.Error(fmt.Sprintf("change status on event %s error: %s", string(governance.EventBind), string(data)))
	}
	if ok, data := rm.RuleManager.ChangeStatus(ruleAddr, string(governance.EventApprove), string(governance.GovernanceBinding), []byte(chainId)); !ok {
		return boltvm.Error(fmt.Sprintf("change status on event %s error: %s", string(governance.EventApprove), string(data)))
	}

	return boltvm.Success(nil)
}

func (rm *RuleManager) bindRule(info *UpdateMasterRuleInfo, event governance.EventType, reason string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub

	// submit proposal
	infoData, err := json.Marshal(info)
	if err != nil {
		return boltvm.Error(fmt.Sprintf("marshal update info error: %v", err))
	}

	res := rm.CrossInvoke(constant.GovernanceContractAddr.String(), "SubmitProposal",
		pb.String(rm.Caller()),
		pb.String(string(event)),
		pb.String(""),
		pb.String(string(RuleMgr)),
		pb.String(RuleKey(info.NewRule.Address)),
		pb.String(string(info.NewRule.Status)),
		pb.String(reason),
		pb.Bytes(infoData),
	)
	if !res.Ok {
		return boltvm.Error("cross invoke SubmitProposal error: " + string(res.Result))
	}

	// change status
	if ok, data := rm.RuleManager.ChangeStatus(info.NewRule.Address, string(governance.EventBind), string(info.NewRule.Status), []byte(info.NewRule.ChainId)); !ok {
		return boltvm.Error("change status error: " + string(data))
	}

	return getGovernanceRet(string(res.Result), nil)
}

// LogoutRule logout the validation rule address with the chain id
func (rm *RuleManager) LogoutRule(chainId string, ruleAddress string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub

	// 1. check permission
	if err := rm.checkPermission(chainId, PermissionSelf, nil); err != nil {
		return boltvm.Error(err.Error())
	}

	// 2. check appchain
	if res := rm.CrossInvoke(constant.AppchainMgrContractAddr.String(), "IsAvailable", pb.String(chainId)); !res.Ok {
		return boltvm.Error("cross invoke IsAvailable error: " + string(res.Result))
	}

	// 3. pre logout
	if ok, data := rm.RuleManager.GovernancePre(ruleAddress, governance.EventLogout, []byte(chainId)); !ok {
		return boltvm.Error("logout prepare error: " + string(data))
	}

	// 4. get rule
	ruleRes := rm.GetRuleByAddr(chainId, ruleAddress)
	if !ruleRes.Ok {
		return boltvm.Error("get rule by addr error: " + string(ruleRes.Result))
	}
	rule := &ruleMgr.Rule{}
	if err := json.Unmarshal(ruleRes.Result, &rule); err != nil {
		return boltvm.Error("unmarshal rule error:" + err.Error())
	}

	// 5. change status
	if ok, data := rm.RuleManager.ChangeStatus(ruleAddress, string(governance.EventLogout), string(rule.Status), []byte(chainId)); !ok {
		return boltvm.Error(string(data))
	}

	return getGovernanceRet("", nil)
}

// CountAvailableRules counts all available rules (should be 0 or 1)
func (rm *RuleManager) CountAvailableRules(chainId string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub
	return responseWrapper(rm.RuleManager.CountAvailable([]byte(chainId)))
}

// CountRules counts all rules of a chain
func (rm *RuleManager) CountRules(chainId string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub
	return responseWrapper(rm.RuleManager.CountAll([]byte(chainId)))
}

// Rules returns all rules of a chain
func (rm *RuleManager) Rules(chainId string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub
	return responseWrapper(rm.RuleManager.All([]byte(chainId)))
}

// GetRuleByAddr returns rule by appchain id and rule address
func (rm *RuleManager) GetRuleByAddr(chainId, ruleAddr string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub
	return responseWrapper(rm.RuleManager.QueryById(ruleAddr, []byte(chainId)))
}

// GetRuleByAddr returns rule by appchain id and rule address
func (rm *RuleManager) GetMasterRule(chainId string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub
	return responseWrapper(rm.RuleManager.GetMaster(chainId))
}

// GetAvailableRuleAddr returns available rule address by appchain id and rule address
func (rm *RuleManager) GetAvailableRuleAddr(chainId string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub
	return responseWrapper(rm.RuleManager.GetAvailableRuleAddress(chainId))
}

func (rm *RuleManager) IsAvailableRule(chainId, ruleAddress string) *boltvm.Response {
	rm.RuleManager.Persister = rm.Stub
	return responseWrapper(rm.RuleManager.IsAvailable(chainId, ruleAddress))
}

func (rm *RuleManager) checkRuleAddress(addr string) error {
	if addr == validator.FabricRuleAddr || addr == validator.SimFabricRuleAddr || addr == validator.HappyRuleAddr {
		return nil
	}

	account1 := rm.Persister.GetAccount(addr)

	account := account1.(ledger.IAccount)
	if account.Code() == nil {
		return fmt.Errorf("the validation rule does not exist")
	}

	return nil
}

func (rm *RuleManager) checkPermission(chainId string, p Permission, specificAddrsData []byte) error {
	res := rm.CrossInvoke(constant.AppchainMgrContractAddr.String(), "GetAppchain", pb.String(chainId))
	if !res.Ok {
		return fmt.Errorf("cross invoke GetAppchain error: %s", string(res.Result))
	}

	app := &appchainMgr.Appchain{}
	if err := json.Unmarshal(res.Result, app); err != nil {
		return fmt.Errorf("unmarshal appchain info error: " + err.Error())
	}

	addr, err := app.GetAdminAddress()
	if err != nil {
		return fmt.Errorf("get addr error: %s", err.Error())
	}

	res = rm.CrossInvoke(constant.RoleContractAddr.String(), "CheckPermission",
		pb.String(string(p)),
		pb.String(addr),
		pb.String(rm.CurrentCaller()),
		pb.Bytes(specificAddrsData))
	if !res.Ok {
		return fmt.Errorf("check permission error: %s", string(res.Result))
	}

	return nil
}
func RuleKey(id string) string {
	return ruleMgr.RULEPREFIX + id
}
