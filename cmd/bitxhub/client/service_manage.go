package client

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/fatih/color"
	service_mgr "github.com/meshplus/bitxhub-core/service-mgr"
	"github.com/meshplus/bitxhub-model/constant"
	"github.com/meshplus/bitxhub-model/pb"
	"github.com/tidwall/gjson"
	"github.com/urfave/cli"
)

func serviceMgrCMD() cli.Command {
	return cli.Command{
		Name:  "service",
		Usage: "service manage command",
		Subcommands: cli.Commands{
			cli.Command{
				Name:  "status",
				Usage: "query service status by chainService id",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:     "id",
						Usage:    "Specify chainService id",
						Required: true,
					},
				},
				Action: getServiceStatusById,
			},
			cli.Command{
				Name:  "appServices",
				Usage: "query services by appchain id",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:     "chainID",
						Usage:    "Specify appchain id",
						Required: true,
					},
				},
				Action: getServiceByChainID,
			},
			cli.Command{
				Name:  "freeze",
				Usage: "freeze service by chainService id",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:     "id",
						Usage:    "Specify chainService id",
						Required: true,
					},
					cli.StringFlag{
						Name:     "reason",
						Usage:    "Specify freeze reason",
						Required: false,
					},
				},
				Action: freezeService,
			},
			cli.Command{
				Name:  "activate",
				Usage: "activate service by chainService id",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:     "id",
						Usage:    "Specify chainService id",
						Required: true,
					},
					cli.StringFlag{
						Name:     "reason",
						Usage:    "Specify activate reason",
						Required: false,
					},
				},
				Action: activateService,
			},
		},
	}
}

func getServiceStatusById(ctx *cli.Context) error {
	id := ctx.String("id")

	receipt, err := invokeBVMContractBySendView(ctx, constant.ServiceMgrContractAddr.String(), "GetServiceInfo", pb.String(id))
	if err != nil {
		return fmt.Errorf("invoke BVM contract failed when get service status by id %s: %w", id, err)
	}

	if receipt.IsSuccess() {
		service := &service_mgr.Service{}
		if err := json.Unmarshal(receipt.Ret, service); err != nil {
			return fmt.Errorf("unmarshal receipt error: %w", err)
		}
		color.Green("service %s is %s", fmt.Sprintf("%s:%s", service.ChainID, service.ServiceID), string(service.Status))
	} else {
		color.Red("get service status error: %s\n", string(receipt.Ret))
	}
	return nil
}

func getServiceByChainID(ctx *cli.Context) error {
	chainID := ctx.String("chainID")

	receipt, err := invokeBVMContractBySendView(ctx, constant.ServiceMgrContractAddr.String(), "GetServicesByAppchainID", pb.String(chainID))
	if err != nil {
		return fmt.Errorf("invoke BVM contract failed when get service by appchainID %s: %w", chainID, err)
	}

	if receipt.IsSuccess() {
		var services []*service_mgr.Service
		if err := json.Unmarshal(receipt.Ret, &services); err != nil {
			return fmt.Errorf("unmarshal receipt error: %w", err)
		}
		printService(services)
	} else {
		color.Red("get service status error: %s\n", string(receipt.Ret))
	}
	return nil
}

func freezeService(ctx *cli.Context) error {
	id := ctx.String("id")
	reason := ctx.String("reason")

	receipt, err := invokeBVMContract(ctx, constant.ServiceMgrContractAddr.String(), "FreezeService", pb.String(id), pb.String(reason))
	if err != nil {
		return fmt.Errorf("invoke BVM contract failed when freeze service %s for %s: %w", id, reason, err)
	}

	if receipt.IsSuccess() {
		proposalId := gjson.Get(string(receipt.Ret), "proposal_id").String()
		color.Green("proposal id is %s", proposalId)
	} else {
		color.Red("freeze service error: %s\n", string(receipt.Ret))
	}
	return nil
}

func activateService(ctx *cli.Context) error {
	id := ctx.String("id")
	reason := ctx.String("reason")

	receipt, err := invokeBVMContract(ctx, constant.ServiceMgrContractAddr.String(), "ActivateService", pb.String(id), pb.String(reason))
	if err != nil {
		return fmt.Errorf("invoke BVM contract failed when activate service %s for %s: %w", id, reason, err)
	}

	if receipt.IsSuccess() {
		proposalId := gjson.Get(string(receipt.Ret), "proposal_id").String()
		color.Green("proposal id is %s", proposalId)
	} else {
		color.Red("activate service error: %s\n", string(receipt.Ret))
	}
	return nil
}

func printService(services []*service_mgr.Service) {
	var table [][]string
	table = append(table, []string{"ChainID", "ServiceID", "Name", "Type", "Intro", "Ordered", "Createtime", "Score", "Status"})

	for _, service := range services {
		table = append(table, []string{
			service.ChainID,
			service.ServiceID,
			service.Name,
			string(service.Type),
			service.Intro,
			strconv.FormatBool(service.Ordered),
			strconv.Itoa(int(service.CreateTime)),
			strconv.FormatFloat(service.Score, 'g', -1, 64),
			string(service.Status),
		})
	}

	fmt.Println("========================================================================================")
	PrintTable(table, true)
}
