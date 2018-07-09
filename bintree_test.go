// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import (
	"testing"
)

func tCheckNode(t *testing.T, tree *binTreeState, index int, state binTreeStateValue) {
	if tree.data[index] != state {
		t.Fatalf("tree item %d was in incorrect state", index)
	}
}

func BenchmarkBinTree(b *testing.B) {
	tree := binTree{
		[]binTreeNode{
			*NewBinTreeNode(
				nodeTypeOr,
				0,
				1, 2,
			),
			*NewBinTreeNode(
				nodeTypeLeaf,
				0,
				0, 0,
			),
			*NewBinTreeNode(
				nodeTypeAnd,
				0,
				3, 4,
			),
			*NewBinTreeNode(
				nodeTypeLeaf,
				2,
				0, 0,
			),
			*NewBinTreeNode(
				nodeTypeNot,
				2,
				5, 0,
			),
			*NewBinTreeNode(
				nodeTypeLeaf,
				4,
				0, 0,
			),
		},
	}

	if err := tree.Validate(); err != nil {
		b.Fatalf("tree is invalid: %s", err)
	}

	state := tree.NewState()

	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		state.Reset()

		state.MarkNode(1, false)
		state.MarkNode(3, true)
		state.MarkNode(5, true)
	}
}

func TestBinTree(t *testing.T) {
	tree := binTree{
		[]binTreeNode{
			*NewBinTreeNode(
				nodeTypeOr,
				0,
				1, 2,
			),
			*NewBinTreeNode(
				nodeTypeLeaf,
				0,
				0, 0,
			),
			*NewBinTreeNode(
				nodeTypeAnd,
				0,
				3, 4,
			),
			*NewBinTreeNode(
				nodeTypeLeaf,
				2,
				0, 0,
			),
			*NewBinTreeNode(
				nodeTypeNot,
				2,
				5, 0,
			),
			*NewBinTreeNode(
				nodeTypeLeaf,
				4,
				0, 0,
			),
		},
	}

	if err := tree.Validate(); err != nil {
		t.Fatalf("tree is invalid: %s", err)
	}

	{
		memTrack := allocTracker{}

		state := tree.NewState()

		memTrack.Start()
		state.MarkNode(1, false)
		state.MarkNode(3, true)
		state.MarkNode(5, true)
		memTrack.Stop()

		if memTrack.Alloc() != 0 {
			t.Fatal("marking nodes should not allocate memory")
		}
	}

	{
		state := tree.NewState()

		state.MarkNode(1, false)
		tCheckNode(t, state, 0, binTreeStateUnknown)
		tCheckNode(t, state, 1, binTreeStateFalse)
		tCheckNode(t, state, 2, binTreeStateUnknown)
		tCheckNode(t, state, 3, binTreeStateUnknown)
		tCheckNode(t, state, 4, binTreeStateUnknown)

		state.MarkNode(3, true)
		tCheckNode(t, state, 0, binTreeStateUnknown)
		tCheckNode(t, state, 1, binTreeStateFalse)
		tCheckNode(t, state, 2, binTreeStateUnknown)
		tCheckNode(t, state, 3, binTreeStateTrue)
		tCheckNode(t, state, 4, binTreeStateUnknown)

		state.MarkNode(5, true)
		tCheckNode(t, state, 0, binTreeStateFalse)
		tCheckNode(t, state, 1, binTreeStateFalse)
		tCheckNode(t, state, 2, binTreeStateFalse)
		tCheckNode(t, state, 3, binTreeStateTrue)
		tCheckNode(t, state, 4, binTreeStateFalse)
		tCheckNode(t, state, 5, binTreeStateTrue)
	}

	{
		state := tree.NewState()

		state.MarkNode(1, true)
		tCheckNode(t, state, 0, binTreeStateTrue)
		tCheckNode(t, state, 1, binTreeStateTrue)
		tCheckNode(t, state, 2, binTreeStateResolved)
		tCheckNode(t, state, 3, binTreeStateResolved)
		tCheckNode(t, state, 4, binTreeStateResolved)
	}

	{
		state := tree.NewState()

		state.MarkNode(1, false)
		tCheckNode(t, state, 0, binTreeStateUnknown)
		tCheckNode(t, state, 1, binTreeStateFalse)
		tCheckNode(t, state, 2, binTreeStateUnknown)
		tCheckNode(t, state, 3, binTreeStateUnknown)
		tCheckNode(t, state, 4, binTreeStateUnknown)

		state.MarkNode(3, true)
		tCheckNode(t, state, 0, binTreeStateUnknown)
		tCheckNode(t, state, 1, binTreeStateFalse)
		tCheckNode(t, state, 2, binTreeStateUnknown)
		tCheckNode(t, state, 3, binTreeStateTrue)
		tCheckNode(t, state, 4, binTreeStateUnknown)

		state.MarkNode(5, false)
		tCheckNode(t, state, 0, binTreeStateTrue)
		tCheckNode(t, state, 1, binTreeStateFalse)
		tCheckNode(t, state, 2, binTreeStateTrue)
		tCheckNode(t, state, 3, binTreeStateTrue)
		tCheckNode(t, state, 4, binTreeStateTrue)
		tCheckNode(t, state, 5, binTreeStateFalse)
	}
}
