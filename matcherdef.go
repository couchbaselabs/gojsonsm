package gojsonsm

import (
	"fmt"
	"sort"
	"strings"
)

type SlotID int
type BucketID int

type SlotRef struct {
	Slot SlotID
}

func (ref SlotRef) String() string {
	return fmt.Sprintf("$%d", ref.Slot)
}

type OpType int

const (
	OpTypeEquals OpType = iota
	OpTypeNotEquals
	OpTypeLessThan
	OpTypeLessEquals
	OpTypeGreaterThan
	OpTypeGreaterEquals
	OpTypeExists
	OpTypeIn
	OpTypeMatches
)

func opTypeToString(value OpType) string {
	switch value {
	case OpTypeEquals:
		return "eq"
	case OpTypeNotEquals:
		return "neq"
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
	}

	return "??unknown??"
}

type OpNode struct {
	BucketIdx BucketID
	Op        OpType
	Rhs       interface{}
}

func (op OpNode) String() string {
	var out string
	out += fmt.Sprintf("[%d] %s", op.BucketIdx, opTypeToString(op.Op))

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
	StoreId SlotID
	Ops     []*OpNode
	Elems   map[string]*ExecNode
	Loops   []LoopNode
	After   map[SlotID]*ExecNode
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

type MatchDef struct {
	ParseNode    *ExecNode
	MatchTree    binTree
	MatchBuckets []int
	NumBuckets   int
	NumSlots     int
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
	out += fmt.Sprintf("num fetches: %d\n", def.NumSlots)
	out += fmt.Sprintf("max depth: %d\n", def.MaxDepth)
	return strings.TrimRight(out, "\n")
}
