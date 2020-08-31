package boltvm

import (
	"github.com/meshplus/bitxhub-kit/types"
	"github.com/meshplus/bitxhub-model/pb"
	"github.com/meshplus/bitxhub/internal/ledger"
	"github.com/sirupsen/logrus"
)

type Context struct {
	Caller           types.Address
	Callee           types.Address
	Ledger           ledger.Ledger
	TransactionIndex uint64
	TransactionHash  types.Hash
	TransactionData  *pb.TransactionData
	Nonce            int64
	Logger           logrus.FieldLogger
	Contract         Contract
}

type Option func(*Context)

func NewContext(tx *pb.Transaction, txIndex uint64, data *pb.TransactionData,
	ledger ledger.Ledger, logger logrus.FieldLogger, opts ...Option) *Context {
	context := &Context{
		Caller:           tx.From,
		Callee:           tx.To,
		Ledger:           ledger,
		TransactionIndex: txIndex,
		TransactionHash:  tx.TransactionHash,
		TransactionData:  data,
		Nonce:            tx.Nonce,
		Logger:           logger,
	}
	for _, opt := range opts {
		opt(context)
	}
	return context
}

// WithContract allows interchain contract instance in contract pool to be passed outside vm
func WithContract(contract Contract) Option {
	return func(context *Context) {
		context.Contract = contract
	}
}
