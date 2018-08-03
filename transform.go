// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"regexp"
)

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
	SlotIdx         SlotID
	BucketIdx       BucketID
	RootExec        *ExecNode
	RootTree        binTree
	NodeMap         map[VariableID]*ExecNode
	ActiveExec      *ExecNode
	ActiveBucketIdx BucketID
	MaxDepth        int
	CurDepth        int
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

func (t *Transformer) storeNode(node *ExecNode) SlotID {
	if node.StoreId == 0 {
		node.StoreId = t.newSlot()
	}
	return node.StoreId
}

func (t *Transformer) makeAfterNode(node *ExecNode, slot SlotID) *ExecNode {
	if node.After == nil {
		node.After = make(map[SlotID]*ExecNode)
	} else {
		foundNode := node.After[slot]
		if foundNode != nil {
			return foundNode
		}
	}

	newNode := &ExecNode{}
	node.After[slot] = newNode
	return newNode
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

func (t *Transformer) newSlot() SlotID {
	newSlotID := t.SlotIdx
	t.SlotIdx++
	return newSlotID + 1
}

func (t *Transformer) transformMergePiece(expr mergeExpr, i int) *ExecNode {
	if i == len(expr.exprs)-1 {
		expr.bucketIDs[i] = t.ActiveBucketIdx
		return t.transformOne(expr.exprs[i])
	}

	baseBucketIdx := t.ActiveBucketIdx
	t.RootTree.data[baseBucketIdx].NodeType = nodeTypeNeor

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

func (t *Transformer) transformLoop(loopType LoopType, varID VariableID, inExpr, subExpr Expression) *ExecNode {
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
			storeId := t.storeNode(execNode)
			execNode = t.makeAfterNode(t.ActiveExec, storeId)
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
		rhsStoreId := t.storeNode(rhsNode)
		return SlotRef{rhsStoreId}
	} else if rhsValue, ok := expr.(ValueExpr); ok {
		val := NewFastVal(rhsValue.Value)
		if val.IsStringLike() {
			val, _ = val.AsJsonString()
		}
		return val
	} else if rhsValue, ok := expr.(RegexExpr); ok {
		regex, err := regexp.Compile(rhsValue.Regex.(string))
		if err != nil {
			return "??ERROR??"
		}
		return NewFastVal(regex)
	} else {
		return "??ERROR??"
	}
}

func (t *Transformer) transformExists(expr ExistsExpr) *ExecNode {
	if lhsField, ok := expr.SubExpr.(FieldExpr); ok {
		execNode := t.getExecNode(lhsField)

		execNode.Ops = append(execNode.Ops, &OpNode{
			t.ActiveBucketIdx,
			OpTypeExists,
			nil,
		})
	} else {
		panic("LHS of a comparison expression must be a FieldExpr")
	}

	return nil
}

func (t *Transformer) transformNotExists(expr NotExistsExpr) *ExecNode {
	return t.transformOne(NotExpr{
		ExistsExpr{
			expr.SubExpr,
		},
	})
}

func (t *Transformer) transformComparison(op OpType, lhs, rhs Expression) *ExecNode {
	if lhsField, ok := lhs.(FieldExpr); ok {
		execNode := t.getExecNode(lhsField)

		lhsRootRefs := rhs.RootRefs()
		if len(lhsRootRefs) > 0 {
			storeId := t.storeNode(execNode)
			execNode = t.makeAfterNode(t.ActiveExec, storeId)
		}

		execNode.Ops = append(execNode.Ops, &OpNode{
			t.ActiveBucketIdx,
			op,
			t.makeRhsParam(rhs),
		})
	} else {
		panic("LHS of a comparison expression must be a FieldExpr")
	}

	return nil
}

func (t *Transformer) transformEquals(expr EqualsExpr) *ExecNode {
	return t.transformComparison(OpTypeEquals, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformNotEquals(expr NotEqualsExpr) *ExecNode {
	return t.transformComparison(OpTypeNotEquals, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformLessThan(expr LessThanExpr) *ExecNode {
	return t.transformComparison(OpTypeLessThan, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformLessEquals(expr LessEqualsExpr) *ExecNode {
	return t.transformComparison(OpTypeLessEquals, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformGreaterThan(expr GreaterThanExpr) *ExecNode {
	return t.transformComparison(OpTypeGreaterThan, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformGreaterEquals(expr GreaterEqualsExpr) *ExecNode {
	return t.transformComparison(OpTypeGreaterEquals, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformLike(expr LikeExpr) *ExecNode {
	return t.transformComparison(OpTypeMatches, expr.Lhs, expr.Rhs)
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
	case ExistsExpr:
		return t.transformExists(expr)
	case NotExistsExpr:
		return t.transformNotExists(expr)
	case EqualsExpr:
		return t.transformEquals(expr)
	case NotEqualsExpr:
		return t.transformNotEquals(expr)
	case LessThanExpr:
		return t.transformLessThan(expr)
	case LessEqualsExpr:
		return t.transformLessEquals(expr)
	case GreaterThanExpr:
		return t.transformGreaterThan(expr)
	case GreaterEqualsExpr:
		return t.transformGreaterEquals(expr)
	case LikeExpr:
		return t.transformLike(expr)
	}
	return nil
}

var AlwaysTrueIdent = -1
var AlwaysFalseIdent = -2

func (t *Transformer) Transform(exprs []Expression) *MatchDef {
	t.RootExec = &ExecNode{}
	t.ActiveExec = t.RootExec
	t.NodeMap = make(map[VariableID]*ExecNode)

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
		t.SlotIdx = 0
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
		NumSlots:     int(t.SlotIdx),
		MaxDepth:     t.MaxDepth,
	}
}
