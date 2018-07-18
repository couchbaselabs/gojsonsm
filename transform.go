// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"fmt"
	"sort"
	"strings"
)

type VariableID int
type BucketID int

type VarRef struct {
	VarIdx VariableID
}

func (ref VarRef) String() string {
	return fmt.Sprintf("$%d", ref.VarIdx)
}

type mergeExpr struct {
	exprs     []Expression
	bucketIDs []BucketID
}

func (expr mergeExpr) String() string {
	if len(expr.exprs) == 0 {
		return "%%ERROR%%"
	} else if len(expr.exprs) == 1 {
		return expr.exprs[0].String()
	} else {
		value := reindentString(expr.exprs[0].String(), "  ")
		for i := 1; i < len(expr.exprs); i++ {
			value += "\nOR\n"
			value += reindentString(expr.exprs[i].String(), "  ")
		}
		return value
	}
}

func (expr mergeExpr) RootRefs() []FieldExpr {
	var out []FieldExpr
	for _, subexpr := range expr.exprs {
		out = rootSetAdd(out, subexpr.RootRefs()...)
	}
	return out
}

type Transformer struct {
	VarIdx              VariableID
	BucketIdx           BucketID
	RootExec            *ExecNode
	RootTree            binTree
	NodeMap             map[int]*ExecNode
	ActiveExec          *ExecNode
	ActiveBucketIdx     BucketID
	PrevActiveBucketIdx BucketID
	MaxDepth            int
	CurDepth            int
}

type OpType int

const (
	OpTypeEquals OpType = iota
	OpTypeLessThan
	OpTypeGreaterThan
	OpTypeIn
)

func opTypeToString(value OpType) string {
	switch value {
	case OpTypeEquals:
		return "eq"
	case OpTypeLessThan:
		return "lt"
	case OpTypeGreaterThan:
		return "gt"
	case OpTypeIn:
		return "in"
	}
	return "??unknown??"
}

type OpNode struct {
	BucketIdx BucketID
	Op        OpType
	OpNot     bool
	Rhs       interface{}
}

func (op OpNode) String() string {
	var out string
	var opNotStr string
	if op.OpNot {
		opNotStr = "not "
	}
	out += fmt.Sprintf("[%d] %s%s", op.BucketIdx, opNotStr, opTypeToString(op.Op))

	if op.Rhs != nil {
		out += " " + fmt.Sprintf("%v", op.Rhs)
	}

	return out
}

type LoopType int

const (
	LoopTypeAny LoopType = iota
	LoopTypeEvery
	LoopTypeAnyEvery
)

func loopTypeToString(value LoopType) string {
	switch value {
	case LoopTypeAny:
		return "any"
	case LoopTypeEvery:
		return "every"
	case LoopTypeAnyEvery:
		return "anyevery"
	}

	return "??unknown??"
}

type LoopNode struct {
	BucketIdx BucketID
	Mode      LoopType
	Node      *ExecNode
}

type ExecNode struct {
	StoreId VariableID
	Ops     []*OpNode
	Elems   map[string]*ExecNode
	Loops   []LoopNode
	After   map[VariableID]*ExecNode
}

type MatchDef struct {
	ParseNode    *ExecNode
	MatchTree    binTree
	MatchBuckets []int
	NumBuckets   int
	NumFetches   int
	MaxDepth     int
}

func (def MatchDef) String() string {
	var out string
	out += "match tree:\n"
	out += reindentString(def.ParseNode.String(), "  ")
	out += "\n"
	out += "bin tree:\n"
	out += reindentString(def.MatchTree.String(), "  ")
	out += "\n"
	out += "match buckets:\n"
	for i, bucketID := range def.MatchBuckets {
		out += fmt.Sprintf("  %d: %d\n", i, bucketID)
	}
	out += fmt.Sprintf("num buckets: %d\n", def.NumBuckets)
	out += fmt.Sprintf("num fetches: %d\n", def.NumFetches)
	out += fmt.Sprintf("max depth: %d\n", def.MaxDepth)
	return strings.TrimRight(out, "\n")
}

func (node *ExecNode) makeStored(t *Transformer) VariableID {
	if node.StoreId == 0 {
		node.StoreId = t.newVariable()
	}
	return node.StoreId
}

func (node *ExecNode) makeAfterNode(varID VariableID) *ExecNode {
	if node.After == nil {
		node.After = make(map[VariableID]*ExecNode)
	} else {
		foundNode := node.After[varID]
		if foundNode != nil {
			return foundNode
		}
	}

	newNode := &ExecNode{}
	node.After[varID] = newNode
	return newNode
}

func (node ExecNode) String() string {
	var out string
	if node.StoreId > 0 {
		out += fmt.Sprintf(":store $%d\n", node.StoreId)
	}

	if len(node.Ops) > 0 {
		out += fmt.Sprintf(":ops\n")
		for _, op := range node.Ops {
			out += reindentString(op.String(), "  ")
			out += "\n"
		}
	}

	// For debugging, lets sort the elements by name first
	var ks []string
	for k := range node.Elems {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	if len(ks) > 0 {
		out += fmt.Sprintf(":elems\n")

		for _, k := range ks {
			elem := node.Elems[k]
			out += fmt.Sprintf("  `%s`:\n", k)
			out += reindentString(elem.String(), "    ")
			out += "\n"
		}
	}

	if node.Loops != nil {
		out += fmt.Sprintf(":loops\n")
		for _, loop := range node.Loops {
			out += fmt.Sprintf("[%d] :%s:\n", loop.BucketIdx, loopTypeToString(loop.Mode))

			out += reindentString(loop.Node.String(), "  ")
			out += "\n"
		}
	}

	if node.After != nil {
		out += fmt.Sprintf(":after:\n")
		for varId, anode := range node.After {
			out += fmt.Sprintf("  #with $%d:\n", varId)
			out += reindentString(anode.String(), "    ")
			out += "\n"
		}
	}

	return strings.TrimRight(out, "\n")
}

func (t *Transformer) getExecNode(field FieldExpr) *ExecNode {
	node := t.RootExec

	if field.Root != 0 {
		node = t.NodeMap[field.Root]
		if node == nil {
			// TODO
			panic("invalid field reference")
		}
	}

	for _, entry := range field.Path {
		if node.Elems == nil {
			node.Elems = make(map[string]*ExecNode)
		} else if newNode, ok := node.Elems[entry]; ok {
			node = newNode
			continue
		}

		newNode := &ExecNode{}
		node.Elems[entry] = newNode
		node = newNode
	}
	return node
}

func (t *Transformer) newBucket() BucketID {
	newBucketIdx := t.BucketIdx
	t.BucketIdx++

	t.RootTree.data = append(t.RootTree.data, *NewBinTreeNode(
		nodeTypeLeaf,
		int(t.ActiveBucketIdx),
		0,
		0,
	))
	t.ActiveBucketIdx = newBucketIdx
	return newBucketIdx
}

func (t *Transformer) newVariable() VariableID {
	newVariableIdx := t.VarIdx
	t.VarIdx++
	return newVariableIdx + 1
}

func (t *Transformer) transformMergePiece(expr mergeExpr, i int) *ExecNode {
	if i == len(expr.exprs)-1 {
		expr.bucketIDs[i] = t.ActiveBucketIdx
		return t.transformOne(expr.exprs[i])
	}

	baseBucketIdx := t.ActiveBucketIdx
	t.RootTree.data[baseBucketIdx].NodeType = nodeTypeOr

	t.newBucket()
	expr.bucketIDs[i] = t.ActiveBucketIdx
	t.RootTree.data[baseBucketIdx].Left = int(t.ActiveBucketIdx)
	t.transformOne(expr.exprs[i])

	t.ActiveBucketIdx = baseBucketIdx
	t.newBucket()
	t.RootTree.data[baseBucketIdx].Right = int(t.ActiveBucketIdx)
	t.transformMergePiece(expr, i+1)

	return nil
}

func (t *Transformer) transformMerge(expr mergeExpr) *ExecNode {
	return t.transformMergePiece(expr, 0)
}

func (t *Transformer) transformNot(expr NotExpr) *ExecNode {
	baseBucketIdx := t.ActiveBucketIdx
	t.RootTree.data[baseBucketIdx].NodeType = nodeTypeNot

	t.newBucket()
	t.RootTree.data[baseBucketIdx].Left = int(t.ActiveBucketIdx)
	t.transformOne(expr.SubExpr)

	return nil
}

func (t *Transformer) transformOr(expr OrExpr) *ExecNode {
	if len(expr) == 1 {
		return t.transformOne(expr[0])
	}

	baseBucketIdx := t.ActiveBucketIdx
	t.RootTree.data[baseBucketIdx].NodeType = nodeTypeOr

	t.newBucket()
	t.RootTree.data[baseBucketIdx].Left = int(t.ActiveBucketIdx)
	t.transformOne(expr[0])

	t.ActiveBucketIdx = baseBucketIdx
	t.newBucket()
	t.RootTree.data[baseBucketIdx].Right = int(t.ActiveBucketIdx)
	t.transformOr(expr[1:])

	return nil
}

func (t *Transformer) transformAnd(expr AndExpr) *ExecNode {
	if len(expr) == 1 {
		return t.transformOne(expr[0])
	}

	baseBucketIdx := t.ActiveBucketIdx
	t.RootTree.data[baseBucketIdx].NodeType = nodeTypeAnd

	t.newBucket()
	t.RootTree.data[baseBucketIdx].Left = int(t.ActiveBucketIdx)
	t.transformOne(expr[0])

	t.ActiveBucketIdx = baseBucketIdx
	t.newBucket()
	t.RootTree.data[baseBucketIdx].Right = int(t.ActiveBucketIdx)
	t.transformAnd(expr[1:])

	return nil
}

func (t *Transformer) transformLoop(loopType LoopType, varID int, inExpr, subExpr Expression) *ExecNode {
	if rhsField, ok := inExpr.(FieldExpr); ok {
		newNode := &ExecNode{}
		execNode := t.getExecNode(rhsField)

		// If the sub-expression of this loop access data that
		// is not whole contained within the loop variables, we
		// need to pull the whole loop out to the after block
		// to guarentee that all data dependencies have been
		// resolved and are available.
		subRootRefs := subExpr.RootRefs()
		if len(subRootRefs) > 0 {
			storeId := execNode.makeStored(t)
			execNode = t.ActiveExec.makeAfterNode(storeId)
		}

		execNode.Loops = append(execNode.Loops, LoopNode{
			t.ActiveBucketIdx,
			loopType,
			newNode,
		})

		oldActiveExec := t.ActiveExec
		t.ActiveExec = newNode
		t.CurDepth++

		if t.CurDepth > t.MaxDepth {
			t.MaxDepth = t.CurDepth
		}

		t.NodeMap[varID] = newNode

		t.transformOne(subExpr)

		t.CurDepth--
		t.ActiveExec = oldActiveExec
	} else {
		panic("RHS of AnyIn must be a FieldExpr")
	}

	return nil
}

func (t *Transformer) transformAnyIn(expr AnyInExpr) *ExecNode {
	return t.transformLoop(LoopTypeAny, expr.VarId, expr.InExpr, expr.SubExpr)
}

func (t *Transformer) transformEveryIn(expr EveryInExpr) *ExecNode {
	return t.transformLoop(LoopTypeEvery, expr.VarId, expr.InExpr, expr.SubExpr)
}

func (t *Transformer) transformAnyEveryIn(expr AnyEveryInExpr) *ExecNode {
	return t.transformLoop(LoopTypeAnyEvery, expr.VarId, expr.InExpr, expr.SubExpr)
}

func (t *Transformer) makeRhsParam(expr Expression) interface{} {
	if rhsField, ok := expr.(FieldExpr); ok {
		rhsNode := t.getExecNode(rhsField)
		rhsStoreId := rhsNode.makeStored(t)
		return VarRef{rhsStoreId}
	} else if rhsValue, ok := expr.(ValueExpr); ok {
		val := NewFastVal(rhsValue.Value)
		if val.IsStringLike() {
			val, _ = val.AsJsonString()
		}
		return val
	} else {
		return "??ERROR??"
	}
}

func (t *Transformer) previousNotNode() bool {
	parentIdx := t.RootTree.data[t.ActiveBucketIdx].ParentIdx
	if parentIdx >= 0 {
		potentialNotNode := t.RootTree.data[parentIdx]
		return potentialNotNode.NodeType == nodeTypeNot
	}
	return false
}

func (t *Transformer) transformEquals(expr EqualsExpr) *ExecNode {
	if lhsField, ok := expr.Lhs.(FieldExpr); ok {
		execNode := t.getExecNode(lhsField)

		lhsRootRefs := expr.Rhs.RootRefs()
		if len(lhsRootRefs) > 0 {
			storeId := execNode.makeStored(t)
			execNode = t.ActiveExec.makeAfterNode(storeId)
		}

		execNode.Ops = append(execNode.Ops, &OpNode{
			t.ActiveBucketIdx,
			OpTypeEquals,
			t.previousNotNode(),
			t.makeRhsParam(expr.Rhs),
		})
	} else {
		panic("LHS of EqualsExpr must be a FieldExpr")
	}

	return nil
}

func (t *Transformer) transformLessThan(expr LessThanExpr) *ExecNode {
	if lhsField, ok := expr.Lhs.(FieldExpr); ok {
		execNode := t.getExecNode(lhsField)

		lhsRootRefs := expr.Rhs.RootRefs()
		if len(lhsRootRefs) > 0 {
			storeId := execNode.makeStored(t)
			execNode = t.ActiveExec.makeAfterNode(storeId)
		}

		execNode.Ops = append(execNode.Ops, &OpNode{
			t.ActiveBucketIdx,
			OpTypeLessThan,
			t.previousNotNode(),
			t.makeRhsParam(expr.Rhs),
		})
	} else {
		panic("LHS of EqualsExpr must be a FieldExpr")
	}

	return nil
}

func (t *Transformer) transformGreaterThan(expr GreaterThanExpr) *ExecNode {
	if lhsField, ok := expr.Lhs.(FieldExpr); ok {
		execNode := t.getExecNode(lhsField)

		lhsRootRefs := expr.Rhs.RootRefs()
		if len(lhsRootRefs) > 0 {
			storeId := execNode.makeStored(t)
			execNode = t.ActiveExec.makeAfterNode(storeId)
		}

		execNode.Ops = append(execNode.Ops, &OpNode{
			t.ActiveBucketIdx,
			OpTypeGreaterThan,
			t.previousNotNode(),
			t.makeRhsParam(expr.Rhs),
		})
	} else {
		panic("LHS of EqualsExpr must be a FieldExpr")
	}

	return nil
}

func (t *Transformer) transformOne(expr Expression) *ExecNode {
	switch expr := expr.(type) {
	case mergeExpr:
		return t.transformMerge(expr)
	case AnyInExpr:
		return t.transformAnyIn(expr)
	case EveryInExpr:
		return t.transformEveryIn(expr)
	case AnyEveryInExpr:
		return t.transformAnyEveryIn(expr)
	case NotExpr:
		return t.transformNot(expr)
	case OrExpr:
		return t.transformOr(expr)
	case AndExpr:
		return t.transformAnd(expr)
	case NotExpr:
		return t.transformNot(expr)
	case EqualsExpr:
		return t.transformEquals(expr)
	case LessThanExpr:
		return t.transformLessThan(expr)
	case GreaterThanExpr:
		return t.transformGreaterThan(expr)
	}
	return nil
}

var AlwaysTrueIdent = -1
var AlwaysFalseIdent = -2

func (t *Transformer) Transform(exprs []Expression) *MatchDef {
	t.RootExec = &ExecNode{}
	t.ActiveExec = t.RootExec
	t.NodeMap = make(map[int]*ExecNode)

	t.CurDepth = 1
	t.MaxDepth = t.CurDepth
	t.BucketIdx = 1
	t.ActiveBucketIdx = 0
	t.RootTree = binTree{[]binTreeNode{
		{
			NodeType: nodeTypeLeaf,
		},
	}}

	// This does two things, it 'predefines' true and false values
	// within it, and then addition provides an index to which generated
	// expression contains the bucket index we need for that expression.
	exprBucketIDs := make([]int, len(exprs))

	var genExprs []Expression
	for i, expr := range exprs {
		switch expr.(type) {
		case TrueExpr:
			exprBucketIDs[i] = AlwaysTrueIdent
			continue
		case FalseExpr:
			exprBucketIDs[i] = AlwaysFalseIdent
			continue
		}

		genExprs = append(genExprs, expr)
		exprBucketIDs[i] = len(genExprs) - 1
	}

	if len(genExprs) > 0 {
		mergeExpr := mergeExpr{
			exprs:     genExprs,
			bucketIDs: make([]BucketID, len(exprs)),
		}
		t.transformOne(mergeExpr)

		for i, index := range exprBucketIDs {
			if index >= 0 {
				exprBucketIDs[i] = int(mergeExpr.bucketIDs[index])
			}
		}
	} else {
		t.RootExec = nil
		t.RootTree = binTree{}
		t.BucketIdx = 0
		t.VarIdx = 0
		t.MaxDepth = 0
	}

	if t.RootExec != nil {
		err := t.RootTree.Validate()
		if err != nil {
			panic(err)
		}

		if t.RootTree.NumNodes() != int(t.BucketIdx) {
			panic("bucket count did not match tree size")
		}
	}

	return &MatchDef{
		ParseNode:    t.RootExec,
		MatchTree:    t.RootTree,
		MatchBuckets: exprBucketIDs,
		NumBuckets:   int(t.BucketIdx),
		NumFetches:   int(t.VarIdx),
		MaxDepth:     t.MaxDepth,
	}
}
