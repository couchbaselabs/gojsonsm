// Copyright 2018 Couchbase, Inc. All rights reserved.

package gojsonsm

import "runtime"

type allocTracker struct {
	before runtime.MemStats
	test   runtime.MemStats
	after  runtime.MemStats
}

func (tracker *allocTracker) Start() {
	runtime.GC()
	runtime.ReadMemStats(&tracker.test)
	runtime.ReadMemStats(&tracker.test)
	runtime.ReadMemStats(&tracker.before)
}

func (tracker *allocTracker) Stop() {
	runtime.ReadMemStats(&tracker.after)
}

func (tracker allocTracker) Alloc() uint64 {
	allocCost := tracker.before.TotalAlloc - tracker.test.TotalAlloc
	return tracker.after.TotalAlloc - tracker.before.TotalAlloc - allocCost
}
