// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type resolvedFieldRef struct {
	Context *compileContext
	Path    []string
}

func (ref resolvedFieldRef) String() string {
	outStr := "$ROOT"
	if ref.Context != nil {
		outStr = fmt.Sprintf("%s", ref.Context)
	}

	if len(ref.Path) > 0 {
		return outStr + "." + strings.Join(ref.Path, ".")
	}

	return outStr
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

type compileContext struct {
	Depth int
	Var   VariableID
	Node  *ExecNode
}

func (ctx *compileContext) String() string {
	return fmt.Sprintf("$%d@%d", ctx.Var, ctx.Depth)
}

type Transformer struct {
	SlotIdx   SlotID
	BucketIdx BucketID
	RootExec  *ExecNode
	RootTree  binTree

	ContextStack    []*compileContext
	ActiveBucketIdx BucketID
}

func (t *Transformer) getExecNode(field resolvedFieldRef) *ExecNode {
	node := t.RootExec
	if field.Context != nil {
		node = field.Context.Node
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

func (t *Transformer) storeExecNode(node *ExecNode) SlotID {
	if node.StoreId == 0 {
		node.StoreId = t.newSlot()
	}
	return node.StoreId
}

func (t *Transformer) getAfterNode(node *ExecNode) *AfterNode {
	if node.After == nil {
		node.After = &AfterNode{}
	}

	return node.After
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

func (t *Transformer) pushContext(varID VariableID, execNode *ExecNode) {
	t.ContextStack = append(t.ContextStack, &compileContext{
		Depth: len(t.ContextStack) + 1,
		Var:   varID,
		Node:  execNode,
	})
}

func (t *Transformer) popContext(execNode *ExecNode) {
	topContext := t.ContextStack[len(t.ContextStack)-1]
	if topContext.Node != execNode {
		panic("unexpected context in the stack")
	}

	t.ContextStack = t.ContextStack[0 : len(t.ContextStack)-1]
}

func (t *Transformer) gatherResolvedFieldRefs(expr Expression) []resolvedFieldRef {
	fieldRefs := fetchExprFieldRefs(expr)

	var resolvedFieldRefs []resolvedFieldRef
	for _, fieldRef := range fieldRefs {
		resolvedFieldRefs = append(resolvedFieldRefs, t.resolveRef(fieldRef))
	}
	return resolvedFieldRefs
}

func (t *Transformer) getContext(varID VariableID) *compileContext {
	if varID == 0 {
		return nil
	}

	for i := len(t.ContextStack) - 1; i >= 0; i++ {
		if t.ContextStack[i].Var == varID {
			return t.ContextStack[i]
		}
	}

	panic("reference to out-of-context variable was encountered")
}

func (t *Transformer) resolveRef(fieldExpr FieldExpr) resolvedFieldRef {
	return resolvedFieldRef{
		Context: t.getContext(fieldExpr.Root),
		Path:    fieldExpr.Path,
	}
}

func (t *Transformer) findFieldRefsBestRoot(fieldRefs []resolvedFieldRef) (resolvedFieldRef, bool) {
	var currentContext *compileContext
	if len(t.ContextStack) > 0 {
		currentContext = t.ContextStack[len(t.ContextStack)-1]
	}

	var contextFields []resolvedFieldRef
	for _, fieldRef := range fieldRefs {
		if fieldRef.Context == currentContext {
			contextFields = append(contextFields, fieldRef)
		}
	}

	if len(contextFields) == 0 {
		return resolvedFieldRef{
			Context: currentContext,
			Path:    []string{},
		}, false
	}

	// Pick the base path as being the longest of all the paths
	basePath := contextFields[0].Path
	for i := 1; i < len(contextFields); i++ {
		if len(contextFields[i].Path) > len(basePath) {
			basePath = contextFields[i].Path
		}
	}

	var commonPath []string

PathLoop:
	for j := 0; j < len(basePath); j++ {
		for i := 0; i < len(contextFields); i++ {
			deepField := contextFields[i]
			if len(deepField.Path) < j || deepField.Path[j] != basePath[j] {
				break PathLoop
			}
		}
		commonPath = append(commonPath, basePath[j])
	}

	needsAfter := len(commonPath) < len(basePath)

	return resolvedFieldRef{
		Context: currentContext,
		Path:    commonPath,
	}, needsAfter
}

type nodeRef struct {
	node  *ExecNode
	after *AfterNode
}

func (ref *nodeRef) AddOp(op OpNode) {
	if ref.node != nil {
		ref.node.Ops = append(ref.node.Ops, op)
	} else if ref.after != nil {
		ref.after.Ops = append(ref.after.Ops, op)
	} else {
		panic("cannot add an op to a null node reference")
	}
}

func (ref *nodeRef) AddLoop(loop LoopNode) {
	// TODO(brett19): This function currently validates that there
	// is only 1 valid possible loop target used depending on which
	// loop type its going into.  Someday we may implement function
	// support which will invalidate this error check.  We do this
	// here to ensure that the error is caught at compilation rather
	// than at match time.

	if ref.node != nil {
		if loop.Target != nil {
			panic("loops must always target the active state")
		}

		ref.node.Loops = append(ref.node.Loops, loop)
	} else if ref.after != nil {
		if _, ok := loop.Target.(SlotRef); !ok {
			panic("after-loops must always target a slot")
		}

		ref.after.Loops = append(ref.after.Loops, loop)
	} else {
		panic("cannot add a loop to a null node reference")
	}
}

func (t *Transformer) pickBaseNode(expr Expression) nodeRef {
	fieldRefs := t.gatherResolvedFieldRefs(expr)
	bestBase, needsAfter := t.findFieldRefsBestRoot(fieldRefs)
	baseNode := t.getExecNode(bestBase)

	if !needsAfter {
		return nodeRef{
			node:  baseNode,
			after: nil,
		}
	}

	afterNode := t.getAfterNode(baseNode)
	return nodeRef{
		node:  nil,
		after: afterNode,
	}
}

func (t *Transformer) makeDataRef(expr Expression, context nodeRef) (DataRef, error) {
	switch expr := expr.(type) {
	case FieldExpr:
		resField := t.resolveRef(expr)
		fieldNode := t.getExecNode(resField)
		if context.node == fieldNode {
			return nil, nil
		}

		slot := t.storeExecNode(fieldNode)
		return SlotRef{slot}, nil
	case ValueExpr:
		val := NewFastVal(expr.Value)
		if val.IsStringLike() {
			val, _ = val.AsJsonString()
		}
		return val, nil
	case RegexExpr:
		regex, err := regexp.Compile(expr.Regex.(string))
		if err != nil {
			return nil, errors.New("failed to compile RegexExpr: " + err.Error())
		}
		return NewFastVal(regex), nil
	}

	return nil, errors.New("unsupported expression in parameter")
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

func (t *Transformer) transformLoop(expr Expression, loopType LoopType, varID VariableID, inExpr, subExpr Expression) *ExecNode {
	baseNode := t.pickBaseNode(expr)

	newNode := &ExecNode{}

	loopTarget, err := t.makeDataRef(inExpr, baseNode)
	if err != nil {
		panic(err)
	}

	baseNode.AddLoop(LoopNode{
		t.ActiveBucketIdx,
		loopType,
		loopTarget,
		newNode,
	})

	// Push this context to the stack
	t.pushContext(varID, newNode)

	// Transform the loops expression body
	t.transformOne(subExpr)

	// Pop from the context stack
	t.popContext(newNode)

	return nil
}

func (t *Transformer) transformAnyIn(expr AnyInExpr) *ExecNode {
	return t.transformLoop(expr, LoopTypeAny, expr.VarId, expr.InExpr, expr.SubExpr)
}

func (t *Transformer) transformEveryIn(expr EveryInExpr) *ExecNode {
	return t.transformLoop(expr, LoopTypeEvery, expr.VarId, expr.InExpr, expr.SubExpr)
}

func (t *Transformer) transformAnyEveryIn(expr AnyEveryInExpr) *ExecNode {
	return t.transformLoop(expr, LoopTypeAnyEvery, expr.VarId, expr.InExpr, expr.SubExpr)
}

func (t *Transformer) transformExists(expr ExistsExpr) *ExecNode {
	baseNode := t.pickBaseNode(expr)

	lhsDataRef, err := t.makeDataRef(expr.SubExpr, baseNode)
	if err != nil {
		panic(err)
	}

	baseNode.AddOp(OpNode{
		t.ActiveBucketIdx,
		OpTypeExists,
		lhsDataRef,
		nil,
	})

	return nil
}

func (t *Transformer) transformNotExists(expr NotExistsExpr) *ExecNode {
	return t.transformOne(NotExpr{
		ExistsExpr{
			expr.SubExpr,
		},
	})
}

func (t *Transformer) transformComparison(expr Expression, op OpType, lhs, rhs Expression) *ExecNode {
	baseNode := t.pickBaseNode(expr)

	lhsRef, err := t.makeDataRef(lhs, baseNode)
	if err != nil {
		panic(err)
	}

	rhsRef, err := t.makeDataRef(rhs, baseNode)
	if err != nil {
		panic(err)
	}

	baseNode.AddOp(OpNode{
		t.ActiveBucketIdx,
		op,
		lhsRef,
		rhsRef,
	})

	return nil
}

func (t *Transformer) transformEquals(expr EqualsExpr) *ExecNode {
	return t.transformComparison(expr, OpTypeEquals, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformNotEquals(expr NotEqualsExpr) *ExecNode {
	return t.transformComparison(expr, OpTypeNotEquals, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformLessThan(expr LessThanExpr) *ExecNode {
	return t.transformComparison(expr, OpTypeLessThan, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformLessEquals(expr LessEqualsExpr) *ExecNode {
	return t.transformComparison(expr, OpTypeLessEquals, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformGreaterThan(expr GreaterThanExpr) *ExecNode {
	return t.transformComparison(expr, OpTypeGreaterThan, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformGreaterEquals(expr GreaterEqualsExpr) *ExecNode {
	return t.transformComparison(expr, OpTypeGreaterEquals, expr.Lhs, expr.Rhs)
}

func (t *Transformer) transformLike(expr LikeExpr) *ExecNode {
	return t.transformComparison(expr, OpTypeMatches, expr.Lhs, expr.Rhs)
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
	t.ContextStack = nil
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
	}
}
