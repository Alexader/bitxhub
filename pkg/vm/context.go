package vm

import (
	"github.com/meshplus/bitxhub-kit/types"
	"github.com/meshplus/bitxhub-model/pb"
	"github.com/meshplus/bitxhub/internal/ledger"
	"github.com/meshplus/bitxhub/pkg/vm/boltvm"
	"github.com/sirupsen/logrus"
)

// Context represents the context of wasm
type Context struct {
	Caller           types.Address
	Callee           types.Address
	Ledger           ledger.Ledger
	TransactionIndex uint64
	TransactionHash  types.Hash
	TransactionData  *pb.TransactionData
	Nonce            int64
	Logger           logrus.FieldLogger
	Contract         boltvm.Contract
	ChCurr           chan bool
	ChNext           chan bool
}

type Option func(*Context)

// NewContext creates a context of wasm instance
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

func WithChCurr(ch chan bool) Option {
	return func(context *Context) {
		context.ChCurr = ch
	}
}

func WithChNext(ch chan bool) Option {
	return func(context *Context) {
		context.ChNext = ch
	}
}

func WithContract(contract boltvm.Contract) Option {
	return func(context *Context) {
		context.Contract = contract
	}
}
