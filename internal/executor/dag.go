package executor

import (
	"github.com/meshplus/bitxhub-core/agency"
	"go.uber.org/atomic"
	"sync"
)

// this will analyse the dependency of txs, and construct a DAG
func (pe *ParallelExecutor) runWithDAG(inter []*IndexedTx, cont map[string]agency.Contract) {
	// set condition variable, current sequence and tx sequence in group into contract instance
	//cont := pe.boltContractPool.Get().(map[string]agency.Contract)
	//
	//for _, vmTx := range inter {
	//	index := vmTx.GetIndex()
	//	receipt := pe.applyTxFunc(index, vmTx.GetTx(), invalidTxs[index], &agency.TxOpt{
	//		Contracts: cont,
	//	})
	//	pe.receipts[index] = receipt
	//}
	//
	//// free contract to pool
	//pe.boltContractPool.Put(cont)
	dag := pe.genDAG(inter)
	pe.runOnDAG(dag, cont)
}

func (pe *ParallelExecutor) genDAG(txs []*IndexedTx) *DAG {
	res := NewDAG()
	existResources := make(map[string]*Vertex)
	for _, tx := range txs {
		newVertex := NewVertex(tx.GetTx().Hash().String(), tx)
		resources := tx.GetTx().GetIBTP().Group.GetKeys()
		for i := 0; i < len(resources); i++ {
			if forward, ok := existResources[resources[i]]; ok {
				// 添加到所有之前使用该资源的节点的一条有向边
				forward.AddEdge(newVertex)
			}
			existResources[resources[i]] = newVertex
		}
	}
	return res
}

func (pe *ParallelExecutor) runOnDAG(dag *DAG, cont map[string]agency.Contract) {
	completed := &sync.Map{}
	count := atomic.Int32{}
	zeroDegreeNodesMap := dag.GetZeroDegreeVertices()
	if len(zeroDegreeNodesMap) == 0 {
		return
	}
	tasks := make(chan *Vertex, 10)
	go func() {
		for ver, _ := range zeroDegreeNodesMap {
			tasks <- ver
		}
	}()

	runTx := func(vertex *Vertex) {
		if _, ok := completed.Load(vertex.ID); ok {
			return
		}
		index := vertex.Value.txIndex
		receipt := pe.applyTxFunc(index, vertex.Value.GetTx(), &agency.TxOpt{
			Contracts: cont,
		})
		pe.receipts[index] = receipt
		// decrease indegree of next node
		for next, _ := range vertex.Children {
			nextVertex := dag.vertices[next]
			newDegree := nextVertex.InDegree.Dec()
			if newDegree == 0 {
				tasks <- nextVertex
			}
		}
		completed.Store(vertex.ID, void{})
		count.Inc()
	}

	for count.Load() < int32(len(dag.vertices)) {
		select {
		case ver := <-tasks:
			go runTx(ver)
		}
	}
}

type DAG struct {
	zeroDegrees map[*Vertex]void
	vertices    map[string]*Vertex
}

// NewDAG creates a new Directed Acyclic Graph or DAG.
func NewDAG() *DAG {
	d := &DAG{
		zeroDegrees: make(map[*Vertex]void),
		vertices:    make(map[string]*Vertex, 0),
	}

	return d
}

func (dag *DAG) GetZeroDegreeVertices() map[*Vertex]void {
	if len(dag.vertices) == 0 {
		return nil
	}
	//zeroDegrees := &sync.Map{}
	//for _, ver := range dag.vertices {
	//	if ver.InDegree.Load() == 0 {
	//		zeroDegrees.Store(ver, void{})
	//	}
	//}
	zeroDegrees := make(map[*Vertex]void)
	for _, ver := range dag.vertices {
		if ver.InDegree.Load() == 0 {
			zeroDegrees[ver] = void{}
		}
	}
	return zeroDegrees
}

func (dag *DAG) RemoveZeros() {
}

type void struct{}

// Vertex type implements a vertex of a Directed Acyclic graph or DAG.
type Vertex struct {
	ID       string
	Value    *IndexedTx
	InDegree atomic.Int32
	Parents  map[string]void
	Children map[string]void
}

// NewVertex creates a new vertex.
func NewVertex(id string, value *IndexedTx) *Vertex {
	v := &Vertex{
		ID:       id,
		Parents:  make(map[string]void),
		Children: make(map[string]void),
		Value:    value,
	}

	return v
}

// Degree return the number of parents and children of the vertex
func (v *Vertex) Degree() int {
	return len(v.Parents) + len(v.Children)
}

//// InDegree return the number of parents of the vertex or the number of edges
//// entering on it.
//func (v *Vertex) InDegree() int {
//	return len(v.Parents)
//}

// OutDegree return the number of children of the vertex or the number of edges
// leaving it.
func (v *Vertex) OutDegree() int {
	return len(v.Children)
}

func (v *Vertex) AddEdge(next *Vertex) {
	if v.ID == next.ID {
		return
	}
	if _, ok := v.Children[next.ID]; ok {
		return
	}
	v.Children[next.ID] = void{}
	next.Parents[v.ID] = void{}
	next.InDegree.Inc()
}

func (v *Vertex) RemoveEdge(next *Vertex) {
	if v.ID == next.ID {
		return
	}
	if _, ok := v.Children[next.ID]; ok {
		return
	}
	v.Children[next.ID] = void{}
	next.Parents[v.ID] = void{}
	next.InDegree.Dec()
}
