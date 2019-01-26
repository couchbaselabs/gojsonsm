package gojsonsm

import (
	"fmt"
	"sort"
	"strings"
)

type SlotID int

func (id SlotID) String() string {
	return fmt.Sprintf("#%d", id)
}

type BucketID int

func (id BucketID) String() string {
	return fmt.Sprintf("%%%d", id)
}

type DataRef interface {
	String() string
}

func dataRefToString(ref DataRef) string {
	if ref == nil {
		return activeLitRef{}.String()
	}
	return ref.String()
}

type activeLitRef struct {
}

func (activeLitRef) String() string {
	return "@"
}

type SlotRef struct {
	Slot SlotID
}

func (ref SlotRef) String() string {
	return fmt.Sprintf("$%d", ref.Slot)
}

type FuncRef struct {
	FuncName string
	Params   []DataRef
}

func (ref FuncRef) String() string {
	value := fmt.Sprintf("func:%s(", ref.FuncName)
	for paramIdx, param := range ref.Params {
		if paramIdx != 0 {
			value += ", "
		}
		value += param.String()
	}
	value += ")"
	return value
}

type OpType int

const (
	OpTypeEquals OpType = iota
	OpTypeLessThan
	OpTypeLessEquals
	OpTypeGreaterThan
	OpTypeGreaterEquals
	OpTypeExists
	OpTypeIn
	OpTypeMatches
)

func (value OpType) String() string {
	switch value {
	case OpTypeEquals:
		return "eq"
	case OpTypeLessThan:
		return "lt"
	case OpTypeLessEquals:
		return "le"
	case OpTypeGreaterThan:
		return "gt"
	case OpTypeGreaterEquals:
		return "gte"
	case OpTypeIn:
		return "in"
	case OpTypeExists:
		return "exists"
	case OpTypeMatches:
		return "matches"
	}

	return "??unknown??"
}

type OpNode struct {
	BucketIdx BucketID
	Op        OpType
	Lhs       DataRef
	Rhs       DataRef
}

func (op OpNode) String() string {
	return fmt.Sprintf("[%d] %s %s %s",
		op.BucketIdx,
		dataRefToString(op.Lhs),
		op.Op,
		dataRefToString(op.Rhs))
}

type LoopType int

const (
	LoopTypeAny LoopType = iota
	LoopTypeEvery
	LoopTypeAnyEvery
)

func (value LoopType) String() string {
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
	Target    DataRef
	Node      *ExecNode
}

func (node *LoopNode) String() string {
	out := ""
	out += fmt.Sprintf("[%d] :%s in %s:\n", node.BucketIdx, node.Mode, dataRefToString(node.Target))
	out += reindentString(node.Node.String(), "  ")
	return out
}

type AfterNode struct {
	Ops   []OpNode
	Loops []LoopNode
}

type ExecNode struct {
	StoreId SlotID
	Elems   map[string]*ExecNode
	Ops     []OpNode
	Loops   []LoopNode
	After   *AfterNode
}

type MatchDef struct {
	ParseNode    *ExecNode
	MatchTree    binTree
	MatchBuckets []int
	NumBuckets   int
	NumSlots     int
}

func (def MatchDef) String() string {
	var out string
	out += "match tree:\n"
	out += "  $doc:\n"
	out += reindentString(def.ParseNode.String(), "    ")
	out += "\n"
	out += "bin tree:\n"
	out += reindentString(def.MatchTree.String(), "  ")
	out += "\n"
	out += "match buckets:\n"
	for i, bucketID := range def.MatchBuckets {
		out += fmt.Sprintf("  %d: %d\n", i, bucketID)
	}
	out += fmt.Sprintf("num buckets: %d\n", def.NumBuckets)
	out += fmt.Sprintf("num slots: %d\n", def.NumSlots)
	return strings.TrimRight(out, "\n")
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

	if len(node.Loops) > 0 {
		out += fmt.Sprintf(":loops\n")
		for _, loop := range node.Loops {
			out += reindentString(loop.String(), "  ")
			out += "\n"
		}
	}

	if node.After != nil {
		if len(node.After.Ops) > 0 {
			out += fmt.Sprintf(":after-ops:\n")
			for _, anode := range node.After.Ops {
				out += reindentString(anode.String(), "  ")
				out += "\n"
			}
		}

		if len(node.After.Loops) > 0 {
			out += fmt.Sprintf(":after-loops:\n")
			for _, loop := range node.After.Loops {
				out += reindentString(loop.String(), "  ")
				out += "\n"
			}
		}
	}

	return strings.TrimRight(out, "\n")
}
