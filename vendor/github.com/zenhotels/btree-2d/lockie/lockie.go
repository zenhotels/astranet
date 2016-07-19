// Package lockie provides a very simple spinlock.
package lockie

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// i64b is a boolean value which represents whether or not the current system is 64-bit
var i64b = is64bit()

// NewLockie returns a Lockie interface
func NewLockie() Lockie {
	// If we are not on 64-bit, return Lockie32
	if !i64b {
		return &Lockie32{}
	}

	// Else, return Lockie64
	return &Lockie64{}
}

// Lockie is the primary interface for locking/unlocking
type Lockie interface {
	Lock()
	Unlock()
}

// NewLockie64 returns a pointer to a new instance of Lockie64
func NewLockie64() *Lockie64 {
	return &Lockie64{}
}

// Lockie64 is the 64-bit optimized locking mechanism
type Lockie64 struct {
	// Lock state
	// 0 represents an unlocked state
	// 1 represents a locked state
	lock int64
}

// Lock acquires a write-lock
func (l *Lockie64) Lock() {
	// Loop until we are able to swap value of l.lock from 0 to 1
	for !atomic.CompareAndSwapInt64(&l.lock, 0, 1) {
		// Allow other go routines to utilize some CPU time
		runtime.Gosched()
	}
}

// Unlock releases a lock
func (l *Lockie64) Unlock() {
	// Swaps the value of l.lock to 0
	atomic.StoreInt64(&l.lock, 0)
}

// NewLockie32 returns a pointer to a new instance of Lockie32
func NewLockie32() *Lockie32 {
	return &Lockie32{}
}

// Lockie32 is the 32-bit optimized locking mechanism
type Lockie32 struct {
	// Lock state
	// 0 represents an unlocked state
	// 1 represents a locked state
	lock int32
}

// Lock acquires a write-lock
func (l *Lockie32) Lock() {
	// Loop until we are able to swap value of l.lock from 0 to 1
	for !atomic.CompareAndSwapInt32(&l.lock, 0, 1) {
		// Allow other go routines to utilize some CPU time
		runtime.Gosched()
	}
}

// Unlock releases a lock
func (l *Lockie32) Unlock() {
	// Swaps the value of l.lock to 0
	atomic.StoreInt32(&l.lock, 0)
}

// Checks to see if the current system is 64-bit
func is64bit() bool {
	var i int
	// If size of int is 8 bytes, we are on a 64-bit system
	// Otherwise, we are on a 32-bit system (4 byte int length)
	return unsafe.Sizeof(&i) == 8
}
