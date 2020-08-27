package parallel

import (
	"fmt"

	"github.com/meshplus/bitxhub/internal/executor/contracts"
	"github.com/meshplus/bitxhub/pkg/vm/boltvm"
)

type ContractPool struct {
	pool []*contracts.ParallelInterchainManager
}

func NewContractPool(size int) *ContractPool {
	pool := make([]*contracts.ParallelInterchainManager, size)
	for i := 0; i < size; i++ {
		pool[i] = contracts.New()
	}
	return &ContractPool{pool: pool}
}

func (c *ContractPool) GetContract() (*contracts.ParallelInterchainManager, error) {
	for _, c := range c.pool {
		status := c.GetStatus().Swap(true)
		if !status {
			return c, nil
		}
	}
	return nil, fmt.Errorf("no available contract")
}

func (c *ContractPool) FreeContract(interchainContract boltvm.Contract) error {
	switch c := interchainContract.(type) {
	case *contracts.ParallelInterchainManager:
		if !c.GetStatus().CAS(true, false) {
			return fmt.Errorf("contract already freed")
		}
		return nil
	default:
		return fmt.Errorf("not parallel contract to free")
	}
}
