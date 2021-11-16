package query

import (
	"context"
	"db/parser/cnosql"
	"io"
)

type planNode struct {
	Expr cnosql.Expr
	Aux  []cnosql.VarRef
	Cost IteratorCost
}

type explainIteratorCreator struct {
	ic interface {
		IteratorCreator
		io.Closer
	}
	nodes []planNode
}

func (e *explainIteratorCreator) CreateIterator(ctx context.Context, m *cnosql.Metric, opt IteratorOptions) (Iterator, error) {
	cost, err := e.ic.IteratorCost(m, opt)
	if err != nil {
		return nil, err
	}
	e.nodes = append(e.nodes, planNode{
		Expr: opt.Expr,
		Aux:  opt.Aux,
		Cost: cost,
	})
	return &nilFloatIterator{}, nil
}

func (e *explainIteratorCreator) IteratorCost(m *cnosql.Metric, opt IteratorOptions) (IteratorCost, error) {
	return e.ic.IteratorCost(m, opt)
}

func (e *explainIteratorCreator) Close() error {
	return e.ic.Close()
}
