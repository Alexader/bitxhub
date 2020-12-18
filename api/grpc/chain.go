package grpc

import (
	"context"
	"github.com/meshplus/bitxhub-model/pb"
)

func (cbs *ChainBrokerService) GetChainMeta(ctx context.Context, req *pb.Request) (*pb.ChainMeta, error) {
	return cbs.api.Chain().Meta()
}

func GetChainStatus(cbs *ChainBrokerService) (*pb.Response, error) {
	return &pb.Response{
		Data: []byte(cbs.api.Chain().Status()),
	}, nil
}

